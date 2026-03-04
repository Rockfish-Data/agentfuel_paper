# AgentFuel Code

Code for the "Generating Expressive and Customizable Evals for Timeseries Data Analysis Agents with AgentFuel" paper.

## Prerequisites

This code assumes the agent and its backing data source are already set up (e.g. a Databricks Genie Space with the relevant tables loaded). 
Contact the owners of this repo for access, or set up your own data sources using the dataset files in `orig_data/`.  

## Setup

Requires Python 3.11 and [uv](https://docs.astral.sh/uv/).

Clone this repository, `cd` into the codebase, and run:

```bash
uv sync
```

## Quickstart

### 1. Create an env file for your agent

Each agent reads credentials from a `.env` file whose path is set in its config YAML.

Currently, the following agents are supported:

- Databricks Genie (`agents/dbrx/`)
- Snowflake Cortex Analyst (`agents/snow/`)
- PandasAI (`agents/pandasai/`)
- Nao (`agents/nao/`)
- Rockfish LangChain (`agents/rflangchain/`)

The default agent config (`configs/agent/dbrx_wind_data_no_context.yaml`) points to `agents/dbrx/wind_data_no_context.env`.
Create that file or update the config to point to your own.

#### Databricks Genie

```
DBRX_HOST=<your-databricks-workspace-url>
DBRX_AUTH_TOKEN=<your-databricks-personal-access-token>
SPACE_ID=<your-genie-space-id>
```

#### Snowflake Cortex Analyst

```
SNOWFLAKE_HOST=<your-snowflake-account-url>
SNOWFLAKE_USER=<your-snowflake-username>
SNOWFLAKE_AUTH_TOKEN=<your-programmatic-access-token>
SNOWFLAKE_SEMANTIC_VIEW=<your-semantic-view>
```

Make sure to [set the appropriate network policy](https://docs.snowflake.com/en/user-guide/network-policies) for your Snowflake Auth Token. 

#### PandasAI

The env file format depends on the inference engine set via `INFERENCE_ENGINE`.

HuggingFace (`INFERENCE_ENGINE=hf`):

```
INFERENCE_ENGINE=hf
MODEL_NAME=<hf-model-id>
MAX_NEW_TOKENS=<max-new-tokens>
TEMPERATURE=<temperature>
```

OpenAI (`INFERENCE_ENGINE=openai`):

```
INFERENCE_ENGINE=openai
MODEL_NAME=<openai-model-id>
OPENAI_API_KEY=<your-openai-api-key>
```

#### Nao

```
NAO_PROJECT_PATH=/path/to/nao_project
NAO_BIN=/path/to/bin/nao
NAO_PROVIDER=<your-nao-provider>
NAO_MODEL_ID=<your-nao-model-id>
NAO_EMAIL=<your-nao-email>
NAO_PASSWORD=<your-nao-password>
NAO_BACKEND_URL=http://localhost:5005
```

`NAO_BACKEND_URL` is optional (default shown). `NAO_PROJECT_PATH` must point to a Nao project directory containing a `nao_config.yaml`.
Create a separate Nao project per dataset so that each agent only sees the relevant schema. Valid providers are `openai`, `anthropic`, and `google`.
Before running the agentfuel script, make sure to run `nao debug` and `nao sync` inside your Nao project directory.

#### Rockfish LangChain

```
ROCKFISH_API_URL=<your-rockfish-api-url>
ROCKFISH_API_KEY=<your-rockfish-api-key>
ROCKFISH_ORGANIZATION_ID=<your-organization-id>
ROCKFISH_PROJECT_ID=<your-project-id>
ROCKFISH_DATASET_ID=<your-dataset-id>
```

### 2. Prepare a workload CSV

Copy `workload_data/.env.example` to `workload_data/.env` and fill in your Rockfish credentials.

Then generate a test suite JSON from your dataset using the Rockfish API. 
Edit `workload_data/generate_rf_test_suite.yaml` to point to your CSV file and set any other options, then run:

```bash
uv run python workload_data/generate_basic_rf_test_suite.py --config workload_data/config/basic/generate_basic_rf_test_suite.yaml
```

This produces a JSON file (default: `test_suite_output.json`) with the following structure:

```json
{
  "dataset_id": "5k3dBWA1k6ERhEtH2jopPu",
  "test_cases": [
    {
      "answer": 17.22,
      "question": "What's the average wind_speed?",
      "variations": [
        "Can you tell me the mean wind speed?",
        "What is the typical wind_speed value?"
      ]
    },
      ...
  ]
}
```

Then convert it to a workload CSV:

```bash
uv run python workload_data/prepare_workload_data.py --test-suite test_suite_output.json --workload-type my_workload
```

This writes `my_workload.csv`:

```csv
query_id,parent_query_id,query_type,query,answer
0,,basic,"What's the average wind_speed?",17.22
1,0,basic,"Can you tell me the mean wind speed?",17.22
2,0,basic,"What is the typical wind_speed value?",17.22
```

The default workload config (`configs/workload/wind_data_test.yaml`) points to `workload_data/wind_data_test.csv`.
Update that file or change the config to use a different path.

### 3. Run the experiment

```bash
uv run python main.py
```

To override config values on the command line (using [Hydra](https://hydra.cc) syntax):

```bash
uv run python main.py agent=dbrx_wind_data_no_context workload=wind_data_test
```

### 4. Inspect the output

Hydra saves outputs under `outputs/<date>/<time>/`. Each run produces:

- `responses.csv`: the workload CSV with an added `response` column containing the agent's text reply for each query
- `responses_evaluated.csv`: `responses.csv` with added columns for each score type, indicating whether the agent's response is correct
- `scores.csv`: summary of scores i.e. correct fraction and total count per score type
- Hydra logs with the full config used for the run

If a query fails, its response value will be `None` and the error will be logged.

To re-run evaluation on existing responses (e.g. after updating an eval function), use `evaluate.py` directly:

```bash
uv run python evaluate.py --results-dir outputs/<date>/<time>
```

Pass `--overwrite` to replace the existing evaluation files; otherwise timestamped files are written alongside them.

### 5. Collect scores across runs

To compare scores across multiple runs, pass a list of Hydra run directories to `collect_scores.py`:

```bash
uv run python collect_scores.py --results-dirs outputs/<date>/<time> outputs/<date>/<time> ...
```

This reads each run's `.hydra/config.yaml` and `scores.csv`, and writes a single CSV to `results/collected_scores_<timestamp>.csv`. 
Each row corresponds to one run. For example:

```csv
agent_type,agent_conf,workload_type,workload_conf,basic_compare_correct_frac,basic_compare_correct_count,basic_compare_total_count,...
pandas_ai,openai_wind_data_no_context,wind_data,workload_data/wind_data.csv,0.83,156,188,...
```

## Configuration

Configs live in `configs/` and follow the [Hydra](https://hydra.cc) defaults list pattern. 
The top-level `configs/config.yaml` selects defaults for `agent` and `workload` groups:

```yaml
defaults:
  - agent: dbrx_wind_data_no_context
  - workload: wind_data_test
```

Add new agent or workload configs under `configs/agent/` and `configs/workload/` respectively.

## Appendix

### Nao backend patch: `experimental_context` not passed in `generate()`

Affects nao-core up to and including commit `c710e5c` (2026-02-14).

The test CLI and `/api/test/run` endpoint call `AgentManager.generate()` in `apps/backend/src/services/agent.service.ts`. 
This function did not pass `experimental_context` to the agent; this field provides the project folder path needed by tool calls. 
This caused the `list`, `search`, `execute_sql` tool calls to fail (as they received `context.projectFolder = undefined`).

Note: The UI does not fail because it calls `/api/agent/chat`, and `stream()` passes the project folder correctly. 

Fix: add the project lookup and `experimental_context` to `generate()`:

```typescript
async generate(messages: UIMessage[]): Promise<AgentRunResult> {
    const startTime = performance.now();
    const project = await retrieveProjectById(this.chat.projectId);
    const result = await this._agent.generate({
        messages: await this._buildModelMessages(messages),
        abortSignal: this._abortController.signal,
        // @ts-expect-error - experimental_context is not yet in the types
        experimental_context: {
            projectFolder: project.path,
        },
    });
```

After fixing the source, rebuild the standalone binary and replace the one bundled in the nao-core Python package:

```bash
cd apps/backend
bun install # if not done previously
bun run build:standalone
cp nao-chat-server /path/to/.venv/lib/python3.11/site-packages/nao_core/bin/nao-chat-server
```
