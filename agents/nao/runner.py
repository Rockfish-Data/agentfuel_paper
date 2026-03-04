import json
import logging
import os
import signal
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import pandas as pd
import requests
from dotenv import load_dotenv

log = logging.getLogger(__name__)

DEFAULT_BACKEND_URL = "http://localhost:5005"


class Runner:
    def __init__(self, env_filepath: str, orig_data_filepath: str):
        load_dotenv(env_filepath)
        self.backend_url = os.getenv("NAO_BACKEND_URL", DEFAULT_BACKEND_URL)
        self.nao_bin_path = os.getenv("NAO_BIN")
        self.project_path = os.getenv("NAO_PROJECT_PATH")
        self.provider = os.getenv("NAO_PROVIDER")
        self.model_id = os.getenv("NAO_MODEL_ID")

        self.process = self.start_backend()
        self.session = self.create_session()

    def start_backend(self) -> subprocess.Popen:
        env = os.environ.copy()
        cmd = [self.nao_bin_path, "chat"]

        nao_log_file = open("/tmp/nao_backend.log", "w")
        process = subprocess.Popen(
            cmd,
            cwd=self.project_path,
            env=env,
            stdout=nao_log_file,
            stderr=nao_log_file,
            start_new_session=True,
            # New process group so all children can be killed together
        )

        try:
            self.wait_for_connection(timeout=30)
            time.sleep(3)
        except RuntimeError as e:
            log.error(f"Cleaning up, error: {e}")
            self.stop_backend(process)

        process_pid = os.getpgid(process.pid)
        log.info(f"Nao backend started, process PID:{process_pid}")
        return process

    def wait_for_connection(self, timeout: int = 30):
        port = int(self.backend_url.rsplit(":", maxsplit=1)[-1])
        for _ in range(timeout * 30):
            try:
                with socket.create_connection(("localhost", port), timeout=0.1):
                    return
            except OSError:
                time.sleep(0.1)

        raise RuntimeError(f"Nao backend failed to start on port:{port}")

    def create_session(self):
        session = requests.Session()

        # Authenticate using nao-email and nao-pwd
        email = os.getenv("NAO_EMAIL")
        password = os.getenv("NAO_PASSWORD")
        if not email or not password:
            raise ValueError(
                "Either run `nao chat` once, or set NAO_EMAIL and NAO_PASSWORD in the env file."
            )
        response = session.post(
            f"{self.backend_url}/api/auth/sign-in/email",
            json={"email": email, "password": password},
        )
        response.raise_for_status()

        return session

    def stop_backend(self, process: subprocess.Popen) -> None:
        process_pid = os.getpgid(process.pid)
        log.info(f"Stopping Nao backend, process PID:{process_pid}")
        os.killpg(process_pid, signal.SIGTERM)

    def run_single_query(self, row) -> Optional[str]:
        query_id = row.query_id
        query = row.query
        try:
            response = self.session.post(
                f"{self.backend_url}/api/test/run",
                json={
                    "prompt": query,
                    "model": {"provider": self.provider, "modelId": self.model_id},
                    "sql": "",
                },
            )
            response.raise_for_status()
            response = response.json()
            log.debug(f"Response for {query_id=}: {json.dumps(response, indent=2)}")
            response_str = response.get("text")
            return response_str
        except requests.HTTPError as e:
            body = e.response.text if e.response is not None else ""
            log.error(f"Error getting response for {query_id=}: {e} | body: {body}")
            return None
        except Exception as e:
            log.error(f"Error getting response for {query_id=}: {e}")
            return None

    def run(self, workload_df: pd.DataFrame):
        try:
            responses = []
            with ThreadPoolExecutor(max_workers=1) as executor:
                futures = [
                    executor.submit(self.run_single_query, row)
                    for row in workload_df.itertuples()
                ]
                for future in futures:
                    responses.append(future.result())
        finally:
            self.stop_backend(self.process)

        return pd.DataFrame(
            {
                "query_id": workload_df["query_id"],
                "query": workload_df["query"],
                "response": responses,
            }
        )
