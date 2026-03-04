import argparse
import time
from pathlib import Path

import pandas as pd
import yaml

from config import Config, AgentConfig, WorkloadConfig

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Script to collect scores across multiple runs and save to results/"
    )
    parser.add_argument(
        "--results-dirs", nargs="+", help="Paths to Hydra run directories"
    )

    args = parser.parse_args()

    # For filename
    timestr = time.strftime("%Y%m%d-%H%M%S")

    result_dict_list = []
    for results_dir in args.results_dirs:
        print(f"Collecting config and scores from {results_dir}...")

        with open(f"{results_dir}/.hydra/config.yaml", "r") as f:
            data = yaml.safe_load(f)
            workload_config = WorkloadConfig(**data["workload"])
            agent_config = AgentConfig(**data["agent"])
            hydra_config = Config(workload=workload_config, agent=agent_config)

        scores_df = pd.read_csv(f"{results_dir}/scores.csv")
        scores_dict = {}
        for row in scores_df.itertuples():
            prefix = row.score_type
            scores_dict[f"{prefix}_correct_frac"] = row.correct_frac
            scores_dict[f"{prefix}_correct_count"] = row.correct_count
            scores_dict[f"{prefix}_total_count"] = row.total_count

        result_dict = {
            "agent_type": hydra_config.agent.agent_type,
            "agent_conf": Path(hydra_config.agent.env_filepath).stem,
            "workload_type": hydra_config.workload.workload_type,
            "workload_conf": Path(hydra_config.workload.data_filepath).stem,
            **scores_dict,
        }
        result_dict_list.append(result_dict)

    results_df = pd.DataFrame(result_dict_list)

    results_output_fp = f"results/collected_scores_{timestr}.csv"
    results_df.to_csv(results_output_fp, index=False)
    print(f"Saved collected scores to {results_output_fp}!")
