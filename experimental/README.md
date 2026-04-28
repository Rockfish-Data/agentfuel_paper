# Experimental Code 

## External Benchmark Analysis

External benchmarks like Spider2-Snow, BIRD LiveSQLBench, and Beaver are analyzed in the respective `analyze_*.ipynb` notebooks. 

The external benchmark data files are also checked in:
1. Spider2-Snow: `spider2-snow.jsonl`, `spider2snow_gold_sql.csv` 
2. BIRD LiveSQLBench: `livesqlbench_data.jsonl`
3. Beaver: `beaver_dev_dw.json`, `beaver_dev_nw.json`
4. KramaBench: JSON files inside the `kramabench_workloads/` directory

To download fresh Spider2-Snow gold SQL from the xlang-ai/Spider2 GitHub repository, set a `GITHUB_PAT` environment variable and run:

```bash
python download_spider2_snow.py
```

`utils.py` contains shared helper functions for evaluating benchmark queries, including keyword-based detection for anomaly and ordering queries.

## Manual Inspection for PandasAI Agents

`manual_benchmarks.py` redefines the stateful and incident workloads (ecommerce, IoT, and telecom) for ease of use.
`run_pandasai_manual_benchmarks.py` runs the PandasAI agent against these workloads and writes results to a `results/` subdirectory. 
It reads model credentials from a `.pandas.env` file in the working directory.

`single_pandasai_check.py` is a one-off script for running a single PandasAI query against a dataset, so you can see the code that the agent has generated for the query. 
It also reads credentials from the `.pandas.env` file in the working directory. 

The `.pandas.env` file should contain the LLM's model name and the API key: 

```
MODEL_NAME=<your-model-name>
API_KEY=<your-model-provider-api-key>
```

Example model names: `o4-mini-2025-04-16`, `claude-sonnet-4-6`, `claude-opus-4-6`.