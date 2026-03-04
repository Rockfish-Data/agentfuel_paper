import os
import pandas as pd
import pandasai as pai
from pandasai import Agent
from pandasai_litellm import LiteLLM
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(".pandas.env")

llm = LiteLLM(
    model=os.getenv("MODEL_NAME"),
    api_key=os.getenv("API_KEY"),
)
pai.config.set({"llm": llm})

ecommerce_data_files = [Path("../orig_data/ecommerce_users_data.csv"), Path("../orig_data/ecommerce_sessions_data.csv")]
iot_data_files = [Path("../orig_data/iot_device_data.csv")]
tel_inc_data_files = [Path("../orig_data/cell_site_with_inc_data.csv"), Path("../orig_data/transport_link_with_inc_data.csv"), Path("../orig_data/core_node_with_inc_data.csv")]
dfs = [pai.DataFrame(pd.read_csv(f), name=f.stem) for f in tel_inc_data_files]

agent = Agent(dfs)

ecommerce_query1 = "How many product views occurred while users had an item in their cart?"
iot_query1 = "Were any v2.0 sensors in warning, critical, maintenance, and then operational? How many?"
tel_inc_query1 = "What was the average latency on the affected router link during the incident?"
tel_inc_query2 = "How many cells lost availability during the outage on January 2, while the core nodes were also under load?"
response = agent.chat(tel_inc_query2, output_type="string")

print(response)
print(agent.last_generated_code)
