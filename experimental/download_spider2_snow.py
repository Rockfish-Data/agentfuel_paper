import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()

REPO_URL = "https://api.github.com/repos/xlang-ai/Spider2/contents/spider2-snow/evaluation_suite/gold/sql"
HEADERS = {
    "Accept": "application/vnd.github.v3+json",
    "Authorization": f"Bearer {os.getenv('GITHUB_PAT')}",
}

response = requests.get(REPO_URL, headers=HEADERS)

if response.status_code != 200:
    raise RuntimeError(f"Error fetching directory: {response.json()}")

files = response.json()

instr_df = pd.read_json("spider2-snow.jsonl", lines=True)
query_dicts = []
for file in files:
    if file["name"].endswith(".sql") and file["type"] == "file":
        raw_url = file["download_url"]

        r = requests.get(raw_url)
        if r.status_code != 200:
            print("Failed:", file["name"])
            continue

        sql_content = r.text.strip()
        query_id = Path(file["name"]).stem
        query = instr_df[instr_df["instance_id"] == query_id]["instruction"].iloc[0]
        query_dicts.append(
            {"instance_id": query_id, "query": query, "sql_query": sql_content}
        )

# Save CSV
df = pd.DataFrame(query_dicts)
output_fp = "spider2snow_gold_sql.csv"
df.to_csv(output_fp, index=False, encoding="utf-8")
print(f"Saved Spider2.0Snow data to {output_fp}!")
