from dataclasses import dataclass
from enum import Enum


WORKLOAD_DATA_HEADERS = [
    "query_id",
    "parent_query_id",
    "query_type",
    "query",
    "answer",
]
RESPONSE_DATA_HEADERS = ["query_id", "query", "response"]


class WorkloadType(Enum):
    WIND_DATA = "wind_data"
    BITCOIN = "bitcoin"
    WEATHER = "weather"
    ECOMMERCE_MANUAL = "ecommerce_manual"
    IOT_MANUAL = "iot_manual"
    TELECOM_MANUAL = "telecom_manual"


@dataclass
class WorkloadConfig:
    workload_type: WorkloadType
    data_filepath: str


class AgentType(Enum):
    DBRX = "dbrx"
    SNOWFLAKE = "snowflake"
    PANDAS_AI = "pandas_ai"
    NAO = "nao"
    RFLANGCHAIN = "rflangchain"


@dataclass
class AgentConfig:
    agent_type: AgentType
    env_filepath: str


@dataclass
class Config:
    workload: WorkloadConfig
    agent: AgentConfig
