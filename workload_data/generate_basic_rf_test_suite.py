import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

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
class BasicConfig:
    csv_file: Optional[str] = None
    dataset_id: Optional[str] = None
    dataset_name: Optional[str] = None
    categorical: List[str] = field(default_factory=list)
    measurement: List[str] = field(default_factory=list)
    timestamp_column: Optional[str] = None
    variations: int = 0
    max_cases: Optional[int] = None
    output: str = "test_suite_output.json"


def generate_test_suite(cfg: BasicConfig):
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

    if cfg.categorical or cfg.measurement:
        schema = {}
        if cfg.categorical:
            schema["categorical_columns"] = cfg.categorical
        if cfg.measurement:
            schema["measurement_columns"] = cfg.measurement
        payload["schema"] = schema

    if cfg.timestamp_column:
        payload["timestamp_column"] = cfg.timestamp_column
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

    # Base timeout 120s, add extra time for variations (~3s per question per variation)
    timeout = 120 + (cfg.variations * 46 * 3)

    print(
        f"Sending request to {ROCKFISH_AGENTFUEL_API_URL}/analytics/generate-test-suite..."
    )
    response = requests.post(
        f"{ROCKFISH_AGENTFUEL_API_URL}/analytics/generate-test-suite",
        headers=headers,
        json=payload,
        timeout=timeout,
    )

    if not response.ok:
        print(f"Error: {response.status_code}")
        print(response.json())
        sys.exit(1)

    # Round numeric answers to three decimal places
    # Add query type "basic" to each test case
    test_suite = response.json()
    for tc in test_suite["test_cases"]:
        if isinstance(tc["answer"], float):
            tc["answer"] = round(tc["answer"], 3)
        tc["query_type"] = "basic"

    return test_suite


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script to create Rockfish test suite")
    parser.add_argument("--config", help="Path to test suite config YAML file")
    args = parser.parse_args()

    if args.config:
        with open(args.config) as f:
            cfg = BasicConfig(**yaml.safe_load(f))
    else:
        raise ValueError("Path to test suite config YAML file required")

    data = generate_test_suite(cfg)

    output_path = Path(__file__).parent / cfg.output
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"{len(data['test_cases'])} basic test cases saved to {output_path}")
    print(f"Original dataset ID: {data['dataset_id']}")
