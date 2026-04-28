import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
import yaml
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ROCKFISH_API_KEY")
PROJECT_ID = os.getenv("ROCKFISH_PROJECT_ID")
ORGANIZATION_ID = os.getenv("ROCKFISH_ORGANIZATION_ID")
ROCKFISH_AGENTFUEL_API_URL = os.getenv(
    "ROCKFISH_AGENTFUEL_API_URL", "https://labs.rockfish.ai"
)


@dataclass
class StatefulConfig:
    csv_file: Optional[str] = None
    dataset_id: Optional[str] = None
    dataset_name: Optional[str] = None
    entity_column: Optional[str] = None
    timestamp_column: Optional[str] = None
    event_type_column: Optional[str] = None
    variations: int = 0
    max_cases: Optional[int] = None
    output: str = "test_suite_output.json"


def generate_test_suite(cfg: StatefulConfig):
    if not cfg.dataset_id and not cfg.csv_file:
        raise ValueError("Config must specify csv_file or dataset_id")

    payload = {}
    if cfg.dataset_id:
        payload["dataset_id"] = cfg.dataset_id
    else:
        with open(cfg.csv_file) as f:
            payload["csv_content"] = f.read()
        if cfg.dataset_name:
            payload["dataset_name"] = cfg.dataset_name

    if cfg.entity_column:
        payload["entity_column"] = cfg.entity_column
    if cfg.timestamp_column:
        payload["timestamp_column"] = cfg.timestamp_column
    if cfg.event_type_column:
        payload["event_type_column"] = cfg.event_type_column
    if cfg.variations > 0:
        payload["variations_per_question"] = cfg.variations
    if cfg.max_cases:
        payload["max_cases"] = cfg.max_cases

    headers = {
        "X-API-Key": f"Bearer {API_KEY}",
        "X-Project-ID": PROJECT_ID,
        "X-Organization-ID": ORGANIZATION_ID,
        "Content-Type": "application/json",
    }

    timeout = 600 + (cfg.variations * 46 * 3)

    print(
        f"Sending request to {ROCKFISH_AGENTFUEL_API_URL}/stateful/generate-test-suite..."
    )
    response = requests.post(
        f"{ROCKFISH_AGENTFUEL_API_URL}/stateful/generate-test-suite",
        headers=headers,
        json=payload,
        timeout=timeout,
    )

    if not response.ok:
        print(f"Error: {response.status_code}")
        print(response.json())
        sys.exit(1)

    test_suite = response.json()
    for tc in test_suite["test_cases"]:
        if isinstance(tc["answer"], float):
            tc["answer"] = round(tc["answer"], 3)
        tc["query_type"] = "stateful"

    return test_suite


def run_multi_config(raw: dict):
    output = raw.get("output", "test_suite_output.json")

    all_test_cases = []
    last_dataset_id = None

    for cfg_dict in raw["configs"]:
        if last_dataset_id and "dataset_id" not in cfg_dict and "csv_file" not in cfg_dict:
            cfg_dict["dataset_id"] = last_dataset_id

        cfg = StatefulConfig(**cfg_dict)
        data = generate_test_suite(cfg)
        last_dataset_id = data["dataset_id"]
        all_test_cases.extend(data["test_cases"])
        print(f"  -> {len(data['test_cases'])} test cases (event_type_column={cfg.event_type_column})")

    return {"dataset_id": last_dataset_id, "test_cases": all_test_cases}, output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to create Rockfish stateful test suite")
    parser.add_argument("--config", help="Path to test suite config YAML file")
    args = parser.parse_args()

    if not args.config:
        raise ValueError("Path to test suite config YAML file required")

    with open(args.config) as f:
        raw = yaml.safe_load(f)

    if "configs" in raw:
        data, output = run_multi_config(raw)
    else:
        cfg = StatefulConfig(**raw)
        data = generate_test_suite(cfg)
        output = cfg.output

    output_stem = Path(output).stem
    output_suffix = Path(output).suffix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{output_stem}_{timestamp}{output_suffix}"

    output_path = Path(__file__).parent / output_filename
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"{len(data['test_cases'])} stateful test cases saved to {output_path}")
    print(f"Original dataset ID: {data['dataset_id']}")
