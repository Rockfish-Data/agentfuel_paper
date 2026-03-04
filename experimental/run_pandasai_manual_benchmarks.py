import logging
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import pandasai as pai
from dotenv import load_dotenv
from pandasai import Agent
from pandasai_litellm import LiteLLM

from manual_benchmarks import BENCHMARKS

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.getLogger("pandasai").setLevel(logging.ERROR)

SCRIPT_DIR = Path(__file__).parent
RESULTS_DIR = SCRIPT_DIR / "results"


def normalize_response(response) -> str:
    if isinstance(response, pd.DataFrame):
        return response.to_string(index=False)
    if hasattr(response, "value"):
        value = response.value
        if isinstance(value, pd.DataFrame):
            return value.to_string(index=False)
        return str(value)
    return str(response)


def load_dfs(data_files: list[dict]) -> list:
    return [pai.DataFrame(pd.read_csv(f["path"]), name=f["name"]) for f in data_files]


def run_benchmark(benchmark: dict, run_dir: Path):
    name = benchmark["name"]
    out_path = run_dir / f"{name}.csv"

    log.info(f"Starting benchmark: {name}")
    dfs = load_dfs(benchmark["data_files"])
    results = []

    for q in benchmark["questions"]:
        log.info(f"[{name}] {q['template']}: {q['query'][:80]}")
        try:
            agent = Agent(dfs)
            agent.start_new_conversation()
            response = agent.chat(q["query"])
            response_str = normalize_response(response)
        except Exception as e:
            log.error(f"Error on '{q['template']}': {e}")
            response_str = f"ERROR: {e}"

        results.append(
            {
                "template": q["template"],
                "query": q["query"],
                "correct_answer": q["correct_answer"],
                "pandasai_response": response_str,
            }
        )

    pd.DataFrame(results).to_csv(out_path, index=False)
    log.info(f"Saved {name} to {out_path}!")


def main():
    env_path = SCRIPT_DIR / ".pandas.env"
    load_dotenv(env_path)

    llm = LiteLLM(
        model=os.getenv("MODEL_NAME"),
        api_key=os.getenv("API_KEY"),
    )
    pai.config.set({"llm": llm})

    now = datetime.now()
    run_dir = RESULTS_DIR / now.strftime("%Y-%m-%d") / now.strftime("%H-%M-%S")
    run_dir.mkdir(parents=True, exist_ok=True)
    log.info(f"Run directory: {run_dir}")

    for benchmark in BENCHMARKS:
        run_benchmark(benchmark, run_dir)

    log.info("All benchmarks complete.")


if __name__ == "__main__":
    main()
