import argparse
import re
import time

import pandas as pd
import torch
from transformers import pipeline, GenerationConfig


def basic_compare(
    expected_list: list[str], actual_list: list[str], query_list: list[str]
) -> list[bool]:
    num_pattern = r"-?\d+\.?\d*"
    results = []
    for exp, act in zip(expected_list, actual_list):
        if pd.isna(act):
            # If there is no response, return False. A null variant should still
            # be a string answer of len > 0
            match = False
        elif exp == "nan":
            # If expected is null, then check if actual is a variant of null
            act_is_null = pd.isna(act)
            act_is_zero_or_null = isinstance(act, str) and (
                any(float(n) == 0 for n in re.findall(num_pattern, act))
                or "nan" in act.lower()
                or "null" in act.lower()
                or "none" in act.lower()
            )
            match = act_is_null or act_is_zero_or_null
        else:
            # Try comparing actual and expected as ints/floats first, fallback to string otherwise
            try:
                act_bare = act.replace(",", "")
                exp_rounded = round(float(exp), 2)

                # Check exact expected value first before rounding
                if exp in act_bare:
                    match = True
                elif (
                    exp_rounded == int(exp_rounded)
                    and str(int(exp_rounded)) in act_bare
                ):
                    match = True
                elif str(exp_rounded) in act_bare:
                    match = True
                else:
                    # Extract all numbers from actual and compare numerically
                    match = False
                    for num_str in re.findall(num_pattern, act_bare):
                        if round(float(num_str), 2) == exp_rounded:
                            match = True
                            break
            except (ValueError, TypeError):
                match = exp in act
        results.append(match)
    return results


def nl_compare(
    expected_list: list[str], actual_list: list[str], query_list: list[str]
) -> list[bool]:
    chats = []
    for query, exp, act in zip(query_list, expected_list, actual_list):
        chat = [
            {
                "role": "user",
                "content": (
                    f"Does the actual answer match the expected answer for the given query?\n"
                    f"Query: {query}\n"
                    f"Expected: {exp}\n"
                    f"Actual: {act}\n\n"
                    f"Guidelines:\n"
                    f"- The actual answer may include explanation or units; focus only on the value.\n"
                    f"- For numeric answers, allow rounding differences up to 2 decimal places.\n"
                    f"- For aggregation queries, treat null or no data as equivalent to 0.\n"
                    f"- For string answers, accept paraphrases and synonyms as correct.\n"
                    f"Return true or false only."
                ),
            }
        ]
        chats.append(chat)

    ppl = pipeline(
        task="text-generation",
        model="google/gemma-3-1b-it",
        device_map="auto",
        dtype=torch.bfloat16,
    )
    ppl.tokenizer.pad_token_id = ppl.tokenizer.eos_token_id
    prompts = [
        ppl.tokenizer.apply_chat_template(
            chat, tokenize=False, add_generation_prompt=True
        )
        for chat in chats
    ]

    gen_config = GenerationConfig(
        max_new_tokens=10,
        temperature=0.3,
        do_sample=True,
    )
    outputs = ppl(
        prompts, generation_config=gen_config, batch_size=16, return_full_text=False
    )

    results = []
    for out in outputs:
        text = out[0]["generated_text"].lower()
        if "true" in text:
            results.append(True)
        else:
            results.append(False)
    return results


SCORE_TYPE_TO_FUNC = {"basic_compare": basic_compare, "nl_compare": nl_compare}


def run_evaluate(
    workload_df: pd.DataFrame, responses_df: pd.DataFrame, score_types: list[str]
):
    eval_df = responses_df.copy(deep=True)
    eval_df["parent_query_id"] = workload_df["parent_query_id"]
    eval_df["query_type"] = workload_df["query_type"]
    eval_df["answer"] = workload_df["answer"]

    query_list = workload_df["query"].tolist()
    expected_list = workload_df["answer"].astype(str).tolist()
    actual_list = responses_df["response"].tolist()

    for sc_type in score_types:
        sc_func = SCORE_TYPE_TO_FUNC[sc_type]
        result_list = sc_func(expected_list, actual_list, query_list)
        eval_df[sc_type] = result_list
    return eval_df


def get_scores(eval_df: pd.DataFrame, score_types: list[str]):
    sc_type_list = score_types

    correct_count_list, correct_frac_list = [], []
    total_count = len(eval_df)
    total_count_list = [total_count] * len(sc_type_list)
    for sc_type in sc_type_list:
        correct_count = eval_df[sc_type].sum()
        correct_frac = round(correct_count / total_count, 2)
        correct_count_list.append(correct_count)
        correct_frac_list.append(correct_frac)

    score_df = pd.DataFrame(
        {
            "score_type": sc_type_list,
            "correct_frac": correct_frac_list,
            "correct_count": correct_count_list,
            "total_count": total_count_list,
        }
    )
    return score_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to re-run evaluation on agent responses"
    )
    parser.add_argument("--results-dir", help="Path to Hydra run directory")

    valid_score_types = list(SCORE_TYPE_TO_FUNC.keys())
    valid_score_types_str = ", ".join(valid_score_types)
    parser.add_argument(
        "--score-types",
        nargs="+",
        default=valid_score_types,
        help=f"Score types to run. Options: {valid_score_types_str}",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Flag to indicate whether original evaluation files should be overwritten",
    )

    args = parser.parse_args()

    invalid = set(args.score_types) - set(valid_score_types)
    if invalid:
        parser.error(
            f"Unknown score types: {', '.join(invalid)}. Valid options: {valid_score_types_str}"
        )

    # For filenames
    timestr = time.strftime("%Y%m%d-%H%M%S")

    results_dir = args.results_dir
    workload_df_path = f"{results_dir}/workload.csv"
    responses_df_path = f"{results_dir}/responses.csv"

    workload_df = pd.read_csv(workload_df_path)
    responses_df = pd.read_csv(responses_df_path)

    print("Evaluating agent responses against ground truth...")
    eval_df = run_evaluate(workload_df, responses_df, args.score_types)

    eval_output_fn = "responses_evaluated"
    if not args.overwrite:
        eval_output_fn += f"_{timestr}"
    eval_df.to_csv(f"{results_dir}/{eval_output_fn}.csv", index=False)

    print("Writing scores...")
    score_df = get_scores(eval_df, args.score_types)

    score_output_fn = "scores"
    if not args.overwrite:
        score_output_fn += f"_{timestr}"
    score_df.to_csv(f"{results_dir}/{score_output_fn}.csv", index=False)

    print("Done!")
