import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)


class Runner:
    def __init__(self, env_filepath: str, orig_data_filepath: str):
        load_dotenv(env_filepath)
        self.host = os.getenv("ROCKFISH_API_URL")
        self.api_key = os.getenv("ROCKFISH_API_KEY")
        self.org_id = os.getenv("ROCKFISH_ORGANIZATION_ID")
        self.proj_id = os.getenv("ROCKFISH_PROJECT_ID")
        self.dataset_id = os.getenv("ROCKFISH_DATASET_ID")

    def run_batch_query(self, batch: pd.DataFrame) -> list[Optional[str]]:
        response = requests.post(
            url=f"{self.host}/customer-llm",
            json={"dataset_id": self.dataset_id, "questions": batch["query"].tolist()},
            headers={
                "X-API-Key": f"Bearer {self.api_key}",
                "X-Project-ID": self.proj_id,
                "X-Organization-ID": self.org_id,
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()
        response = response.json()
        return response["answers"]

    def run(self, workload_df: pd.DataFrame):
        responses = []

        requests_per_batch = 25
        chunk_labels = pd.RangeIndex(len(workload_df)) // requests_per_batch
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(self.run_batch_query, chunk)
                for _, chunk in workload_df.groupby(chunk_labels)
            ]
            for future in futures:
                responses.extend(future.result())

        return pd.DataFrame(
            {
                "query_id": workload_df["query_id"],
                "query": workload_df["query"],
                "response": responses,
            }
        )
