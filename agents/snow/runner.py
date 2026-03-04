import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from urllib.parse import urlparse

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv

log = logging.getLogger(__name__)


class Runner:
    def __init__(self, env_filepath: str, orig_data_filepath: str):
        load_dotenv(env_filepath)
        self.host = os.getenv("SNOWFLAKE_HOST")
        self.token = os.getenv("SNOWFLAKE_AUTH_TOKEN")
        self.user = os.getenv("SNOWFLAKE_USER")
        self.semantic_view = os.getenv("SNOWFLAKE_SEMANTIC_VIEW")
        self.orig_data_filepath = orig_data_filepath
        self.account = urlparse(self.host).hostname.removesuffix(
            ".snowflakecomputing.com"
        )

    def get_analyst_response(self, query: str):
        response = requests.post(
            url=f"{self.host}/api/v2/cortex/analyst/message",
            json={
                "messages": [
                    {"role": "user", "content": [{"type": "text", "text": query}]}
                ],
                "semantic_view": self.semantic_view,
            },
            headers={
                "Authorization": f"Bearer {self.token}",
                "X-Snowflake-Authorization-Token-Type": "PROGRAMMATIC_ACCESS_TOKEN",
                "Content-Type": "application/json",
            },
        )
        response.raise_for_status()
        response = response.json()
        return response

    def execute_sql(self, sql: str) -> pd.DataFrame:
        conn = snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.token,
        )
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(rows, columns=columns)
        finally:
            conn.close()

    def run_single_query(self, row) -> tuple[Optional[str], Optional[str]]:
        query_id = row.query_id
        query = row.query

        try:
            response = self.get_analyst_response(query)
            log.debug(f"Response for {query_id=}: {json.dumps(response, indent=2)}")

            sql = None
            text = None
            for item in response["message"]["content"]:
                if item["type"] == "sql" and sql is None:
                    sql = item["statement"]
                elif item["type"] == "text" and text is None:
                    text = item["text"]

            time.sleep(5)

            if sql:
                result_df = self.execute_sql(sql)
                log.debug(f"SQL result for {query_id=}:\n{result_df}")
                if result_df.shape == (1, 1):
                    return str(result_df.iloc[0, 0]), sql
                return result_df.to_json(orient="records"), sql
            return text, sql
        except Exception as e:
            log.error(f"Error getting response for {query_id=}: {e}")
            return None, None

    def run(self, workload_df: pd.DataFrame):
        responses = []
        sql_queries = []
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = [
                executor.submit(self.run_single_query, row)
                for row in workload_df.itertuples()
            ]
            for future in futures:
                response_str, sql_str = future.result()
                responses.append(response_str)
                sql_queries.append(sql_str)

        return pd.DataFrame(
            {
                "query_id": workload_df["query_id"],
                "query": workload_df["query"],
                "response": responses,
                "sql": sql_queries,
            }
        )
