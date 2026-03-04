import logging
import os
from enum import Enum

import pandas as pd
import pandasai as pai
import torch
from dotenv import load_dotenv
from pandasai import Agent
from pandasai.agent.state import AgentState
from pandasai.core.prompts import BasePrompt
from pandasai.llm.base import LLM
from pandasai_litellm import LiteLLM
from transformers import pipeline, GenerationConfig

log = logging.getLogger(__name__)
logging.getLogger("pandasai").setLevel(logging.ERROR)


class HuggingFaceLLM(LLM):
    def __init__(
        self, model_name: str, max_new_tokens: int, temperature: float, **kwargs
    ):
        super().__init__(api_key=None)
        self.max_new_tokens = max_new_tokens
        self.temperature = temperature

        logging.getLogger("httpx").setLevel(logging.ERROR)
        logging.getLogger("accelerate").setLevel(logging.ERROR)

        self.pipeline = pipeline(
            task="text-generation",
            model=model_name,
            device_map="auto",
            dtype=torch.bfloat16,
            **kwargs,
        )
        self.pipeline.tokenizer.pad_token_id = self.pipeline.tokenizer.eos_token_id

    @property
    def type(self) -> str:
        return "hf"

    def call(self, instruction: BasePrompt, context: AgentState = None) -> str:
        user_prompt = instruction.to_string()

        gen_config = GenerationConfig(
            max_new_tokens=self.max_new_tokens,
            temperature=self.temperature,
            do_sample=True,
        )
        outputs = self.pipeline(
            user_prompt,
            generation_config=gen_config,
            batch_size=1,
            return_full_text=False,
        )

        return outputs[0]["generated_text"]


class InfEngine(Enum):
    HF = "hf"
    LITELLM = "litellm"
    OPENAI = "openai"


class Runner:
    def __init__(self, env_filepath: str, orig_data_filepath: str):
        load_dotenv(env_filepath)

        llm = None
        inference_engine = os.getenv("INFERENCE_ENGINE", default="hf")
        if inference_engine == InfEngine.HF.value:
            llm = HuggingFaceLLM(
                model_name=os.getenv("MODEL_NAME"),
                max_new_tokens=int(os.getenv("MAX_NEW_TOKENS")),
                temperature=float(os.getenv("TEMPERATURE")),
            )
        elif inference_engine == InfEngine.OPENAI.value:
            llm = LiteLLM(
                model=os.getenv("MODEL_NAME"), api_key=os.getenv("OPENAI_API_KEY")
            )
        elif inference_engine == InfEngine.LITELLM.value:
            raise NotImplementedError

        if llm:
            pai.config.set({"llm": llm})
        else:
            raise ValueError("Could not initialize LLM for PandasAI")

        self.orig_data_filepath = orig_data_filepath

    def run(self, workload_df: pd.DataFrame):
        orig_data_df = pai.read_csv(self.orig_data_filepath)
        agent = Agent(orig_data_df)

        responses = []
        for row in workload_df.itertuples():
            query_id = row.query_id
            query = row.query

            try:
                agent.start_new_conversation()
                response_str = agent.chat(query, output_type="string").value
                log.debug(f"Response for {query_id=}: {response_str}")
                responses.append(response_str)
            except Exception as e:
                log.error(f"Error getting response for {query_id=}: {e}")
                responses.append(None)

        return pd.DataFrame(
            {
                "query_id": workload_df["query_id"],
                "query": workload_df["query"],
                "response": responses,
            }
        )
