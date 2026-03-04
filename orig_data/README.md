# Creating Datasets For AgentFuel Benchmarks

Raw datasets used as the backing data for agent benchmarks. The CSV files are pre-generated and checked in; you do not need to regenerate them unless you want fresh data.

## InfluxDB Sample Datasets

`bitcoin.csv`, `weather.csv`, and `wind_data.csv` are downloaded from InfluxDB's public sample data and converted from line protocol format to CSV.

To regenerate them, run from this directory:

```bash
python collect_influx_sample_data.py
```

## Rockfish Synthetic Datasets

The ecommerce, IoT, and telecom datasets are generated using Rockfish. Install the dependencies first in a fresh virtual env:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements_rf.txt
```

Each script reads Rockfish credentials from a `.env` file in the working directory via `python-dotenv`. 
Set the appropriate variables before running:
```
ROCKFISH_API_KEY=<your-rockfish-api-key>
ROCKFISH_PROJECT_ID=<your-rockfish-project-id>
ROCKFISH_ORGANIZATION_ID=<your-rockfish-organization-id>
ROCKFISH_API_URL=https://api.rockfish.ai
```

- `generate_rf_ecommerce_data.py` produces `ecommerce_users_data.csv` and `ecommerce_sessions_data.csv`
- `generate_rf_iot_data.py` produces `iot_device_data.csv`
- `generate_rf_telecom_ran_data.py` produces `cell_site_data.csv`, `cell_site_with_inc_data.csv`, `transport_link_data.csv`, `transport_link_with_inc_data.csv`, `core_node_data.csv`, and `core_node_with_inc_data.csv`

The `_with_inc_` variants include injected incident events and are used by the telecom incident workload.
