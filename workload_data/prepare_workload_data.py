import argparse
import json
from pathlib import Path

import pandas as pd

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        "Script to create workload from a Rockfish test suite"
    )
    parser.add_argument("--test-suite", help="Path to test suite JSON file")
    parser.add_argument(
        "--workload-type", help="Output file prefix (e.g. wind_data, weather, bitcoin)"
    )
    args = parser.parse_args()

    with open(args.test_suite, "r") as f:
        test_suite_json = json.load(f)

    id_list = []
    parent_id_list = []
    query_type_list = []
    query_list = []
    answer_list = []

    i = 0
    test_cases = test_suite_json["test_cases"]
    for tc in test_cases:
        query = tc["question"]
        answer = tc["answer"]
        query_type = tc["query_type"]

        id_list.append(i)
        parent_id_list.append(None)
        query_type_list.append(query_type)
        query_list.append(query)
        answer_list.append(answer)

        parent_id = i
        i = i + 1
        for tc_var in tc["variations"]:
            id_list.append(i)
            parent_id_list.append(parent_id)
            query_type_list.append(query_type)
            query_list.append(tc_var)
            answer_list.append(answer)
            i = i + 1

    df = pd.DataFrame(
        {
            "query_id": id_list,
            "parent_query_id": parent_id_list,
            "query_type": query_type_list,
            "query": query_list,
            "answer": answer_list,
        }
    )

    output_fp = Path(__file__).parent / f"{args.workload_type}.csv"
    df.to_csv(output_fp, index=False)
