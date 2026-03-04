import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI
from dotenv import load_dotenv

log = logging.getLogger(__name__)


class Runner:
    def __init__(self, env_filepath: str, orig_data_filepath: str):
        load_dotenv(env_filepath)
        w = WorkspaceClient(
            host=os.getenv("DBRX_HOST"), token=os.getenv("DBRX_AUTH_TOKEN")
        )
        self.genie_api_client = GenieAPI(w.api_client)
        self.space_id = os.getenv("SPACE_ID")
        self.orig_data_filepath = orig_data_filepath

    def run_single_query(self, row) -> tuple[Optional[str], Optional[str]]:
        query_id = row.query_id
        query = row.query

        try:
            response = self.genie_api_client.start_conversation_and_wait(
                space_id=self.space_id, content=query
            )
            log.debug(
                f"Response for {query_id=}: {json.dumps(response.as_dict(), indent=2)}"
            )

            response_str = None
            sql_str = None
            for attachment in response.attachments:
                if attachment.text and response_str is None:
                    response_str = str(attachment.text.content)
                if attachment.query and sql_str is None:
                    sql_str = attachment.query.query
            return response_str, sql_str
        except Exception as e:
            log.error(f"Error getting response for {query_id=}: {e}")
            return None, None

    def run(self, workload_df: pd.DataFrame):
        responses = []
        sql_queries = []

        # Start a separate thread for each query and wait for response
        # Extract answer from response and store it
        # API rate limit: 5 conversations per minute, so we t(h)read carefully :)
        requests_per_min = 5
        chunk_labels = pd.RangeIndex(len(workload_df)) // requests_per_min
        for _, chunk in workload_df.groupby(chunk_labels):
            start_time = time.time()
            with ThreadPoolExecutor(max_workers=requests_per_min) as executor:
                futures = [
                    executor.submit(self.run_single_query, row)
                    for row in chunk.itertuples()
                ]
                for future in futures:
                    response_str, sql_str = future.result()
                    responses.append(response_str)
                    sql_queries.append(sql_str)

            elapsed_time = time.time() - start_time
            if elapsed_time < 60:
                time.sleep(60 - elapsed_time)

        return pd.DataFrame(
            {
                "query_id": workload_df["query_id"],
                "query": workload_df["query"],
                "response": responses,
                "sql": sql_queries,
            }
        )
