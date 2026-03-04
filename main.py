import logging

import hydra
import pandas as pd
from hydra.core.config_store import ConfigStore
from omegaconf import OmegaConf

from agents.dbrx.runner import Runner as DBRXRunner
from agents.snow.runner import Runner as SnowRunner
from agents.pandasai.runner import Runner as PandasAIRunner
from agents.nao.runner import Runner as NaoRunner
from agents.rflangchain.runner import Runner as RFLangChainRunner
from config import AgentType, Config, WORKLOAD_DATA_HEADERS
from evaluate import run_evaluate, get_scores

log = logging.getLogger(__name__)


AGENT_RUNNER_CLASS = {
    AgentType.DBRX.value: DBRXRunner,
    AgentType.SNOWFLAKE.value: SnowRunner,
    AgentType.PANDAS_AI.value: PandasAIRunner,
    AgentType.NAO.value: NaoRunner,
    AgentType.RFLANGCHAIN.value: RFLangChainRunner,
}

cs = ConfigStore.instance()
cs.store(name="af_config", node=Config)


@hydra.main(version_base=None, config_path="configs", config_name="config")
def main(cfg: Config) -> None:
    log.info(f"Running experiment with settings:\n{OmegaConf.to_yaml(cfg)}")

    output_dir = hydra.core.hydra_config.HydraConfig.get().runtime.output_dir
    log.info(f"Output will be saved to:{output_dir}")

    log.info("Loading workload data...")
    workload_df = pd.read_csv(cfg.workload.data_filepath)
    for expected_col in WORKLOAD_DATA_HEADERS:
        if expected_col not in workload_df.columns:
            raise ValueError(
                f"Workload data must contain columns: {','.join(WORKLOAD_DATA_HEADERS)}. "
                f"Please run prepare_workload_data.py!"
            )
    workload_df.to_csv(
        f"{output_dir}/workload.csv"
    )  # Easier to re-run evaluation, if needed

    workload_type = cfg.workload.workload_type
    orig_data_filepath = f"orig_data/{workload_type}.csv"

    log.info("Setting up agent...")
    agent_type = cfg.agent.agent_type
    agent_runner = AGENT_RUNNER_CLASS[agent_type](
        cfg.agent.env_filepath, orig_data_filepath
    )

    log.info("Running the agent on the workload...")
    responses_df = agent_runner.run(workload_df)
    responses_df.to_csv(f"{output_dir}/responses.csv", index=False)

    log.info("Evaluating agent responses against ground truth...")
    eval_df = run_evaluate(workload_df, responses_df, score_types=["basic_compare"])
    eval_df.to_csv(f"{output_dir}/responses_evaluated.csv", index=False)

    log.info("Writing scores...")
    score_df = get_scores(eval_df, score_types=["basic_compare"])
    score_df.to_csv(f"{output_dir}/scores.csv", index=False)

    log.info("Done!")


if __name__ == "__main__":
    main()
