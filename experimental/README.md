# Experimental Code 

## External Benchmark Analysis

`analyze_bird_livesqlbench-base-full-v1.ipynb` and `analyze_spider2_snow.ipynb` analyze results from the Bird LiveSQLBench and Spider2-Snow benchmarks respectively. 
The corresponding data files (`livesqlbench_data.jsonl`, `spider2-snow.jsonl`, `spider2snow_gold_sql.csv`) are checked in.

To download fresh Spider2-Snow gold SQL from the xlang-ai/Spider2 GitHub repository, set a `GITHUB_PAT` environment variable and run:

```bash
python download_spider2_snow.py
```

`utils.py` contains shared helper functions for evaluating agent responses, including keyword-based detection for anomaly and ordering queries.

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