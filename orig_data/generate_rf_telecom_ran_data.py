import asyncio
import warnings

import numpy as np
import pandas as pd
import rockfish as rf
import rockfish.actions as ra
from dotenv import load_dotenv
from rockfish.actions.ent import (
    CategoricalParams,
    Column,
    ColumnCategoryType,
    ColumnType,
    DataSchema,
    Derivation,
    DerivationFunctionType,
    Domain,
    DomainType,
    Entity,
    EntityRelationship,
    EntityRelationshipType,
    GlobalTimestamp,
    IDParams,
    MapValuesParams,
    SampleFromColumnParams,
    StateMachineParams,
    SumParams,
    TimeseriesParams,
    Timestamp,
    Transition,
    UniformDistParams,
)

load_dotenv()
warnings.filterwarnings("ignore")


def get_telecom_ran_schema(
    n_transport_links=6,
    n_core_nodes=16,
    n_cell_sites=100,
    global_start_time="2026-01-01T00:00:00Z",
    global_end_time="2026-01-03T00:00:00Z",
    global_time_interval="15min",
) -> DataSchema:
    """Create the telecom RAN network schema."""
    # ENTITY 1: transport_link
    transport_link = Entity(
        name="transport_link",
        cardinality=n_transport_links,
        timestamp=Timestamp(column_name="Timestamp", data_type="timestamp"),
        columns=[
            # Metadata columns
            Column(
                name="Device_ID",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=["RTR_001", "RTR_002", "RTR_003", "RTR_004"],
                        with_replacement=True,
                        seed=100,
                    ),
                ),
            ),
            Column(
                name="Interface_ID",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=["eth0", "eth1", "eth2", "eth3"],
                        with_replacement=True,
                        seed=101,
                    ),
                ),
            ),
            # Measurement columns (stateful)
            Column(
                name="Bandwidth_Utilization_Out",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=55.0,
                        min_value=20.0,
                        max_value=90.0,
                        seasonality_type="peak_offpeak",
                        peak_start_hour=8,
                        peak_end_hour=22,
                        seasonality_strength=0.4,
                        noise_level=0.15,
                        spike_probability=0.02,
                        spike_magnitude=0.3,
                        interval_minutes=15,
                        seed=102,
                    ),
                ),
            ),
            Column(
                name="Packet_Loss_Percent",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=0.5,
                        min_value=0.0,
                        max_value=2.0,
                        seasonality_type="none",
                        noise_level=0.3,
                        spike_probability=0.05,
                        spike_magnitude=0.5,
                        interval_minutes=15,
                        seed=103,
                    ),
                ),
            ),
            Column(
                name="Latency_ms",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=15.0,
                        min_value=8.0,
                        max_value=25.0,
                        seasonality_type="peak_offpeak",
                        peak_start_hour=8,
                        peak_end_hour=22,
                        seasonality_strength=0.25,
                        noise_level=0.2,
                        spike_probability=0.03,
                        spike_magnitude=0.4,
                        interval_minutes=15,
                        seed=104,
                    ),
                ),
            ),
            Column(
                name="Jitter_ms",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=2.0,
                        min_value=0.5,
                        max_value=5.0,
                        seasonality_type="none",
                        noise_level=0.25,
                        spike_probability=0.04,
                        spike_magnitude=0.3,
                        interval_minutes=15,
                        seed=105,
                    ),
                ),
            ),
        ],
    )

    # ENTITY 2: core_node
    core_node = Entity(
        name="core_node",
        cardinality=n_core_nodes,
        timestamp=Timestamp(column_name="Timestamp", data_type="timestamp"),
        columns=[
            # Metadata columns
            Column(
                name="Core_Node_ID",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=[
                            "MME_001",
                            "MME_002",
                            "AMF_001",
                            "SMF_001",
                            "UPF_001",
                            "UPF_002",
                        ],
                        with_replacement=False,
                        seed=300,
                    ),
                ),
            ),
            # Measurement columns (stateful)
            Column(
                name="MM_AttachedUEs",
                data_type="int64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=5000.0,
                        min_value=3000.0,
                        max_value=7500.0,
                        seasonality_type="peak_offpeak",
                        peak_start_hour=8,
                        peak_end_hour=22,
                        seasonality_strength=0.3,
                        noise_level=0.15,
                        spike_probability=0.01,
                        spike_magnitude=0.2,
                        interval_minutes=15,
                        seed=302,
                    ),
                ),
            ),
            Column(
                name="SM_ActivePDUSessions",
                data_type="int64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=3000.0,
                        min_value=1800.0,
                        max_value=4500.0,
                        seasonality_type="peak_offpeak",
                        peak_start_hour=8,
                        peak_end_hour=22,
                        seasonality_strength=0.3,
                        noise_level=0.15,
                        spike_probability=0.01,
                        spike_magnitude=0.2,
                        interval_minutes=15,
                        seed=303,
                    ),
                ),
            ),
            Column(
                name="CPU_Load",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=60.0,
                        min_value=30.0,
                        max_value=90.0,
                        seasonality_type="peak_offpeak",
                        peak_start_hour=8,
                        peak_end_hour=22,
                        seasonality_strength=0.35,
                        noise_level=0.2,
                        spike_probability=0.03,
                        spike_magnitude=0.25,
                        interval_minutes=15,
                        seed=304,
                    ),
                ),
            ),
        ],
    )

    # ENTITY 3: cell_site
    cell_site = Entity(
        name="cell_site",
        cardinality=n_cell_sites,
        timestamp=Timestamp(column_name="Timestamp", data_type="timestamp"),
        columns=[
            # Metadata columns
            Column(
                name="Cell_ID",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.ID,
                    params=IDParams(template_str="CELL_{id}"),
                ),
            ),
            # Base_Station_ID references RAN base station (eNodeB for 4G / gNodeB for 5G)
            # This is independent since we don't model base_station as a separate entity
            Column(
                name="Base_Station_ID",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=["eNB_001", "eNB_002", "eNB_003", "gNB_001", "gNB_002"],
                        with_replacement=True,
                        seed=200,
                    ),
                ),
            ),
            Column(
                name="Location_Lat",
                data_type="float64",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.UNIFORM_DIST,
                    params=UniformDistParams(lower=40.0, upper=41.0, seed=202),
                ),
            ),
            Column(
                name="Location_Lon",
                data_type="float64",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.UNIFORM_DIST,
                    params=UniformDistParams(lower=-80.0, upper=-79.0, seed=203),
                ),
            ),
            # Foreign keys to transport_link
            Column(
                name="Transport_Device_ID",
                data_type="string",
                column_type=ColumnType.FOREIGN_KEY,
                column_category_type=ColumnCategoryType.METADATA,
            ),
            Column(
                name="Transport_Interface_ID",
                data_type="string",
                column_type=ColumnType.FOREIGN_KEY,
                column_category_type=ColumnCategoryType.METADATA,
            ),
            # Measurement columns (stateful)
            Column(
                name="RRC_ConnEstabFail",
                data_type="int64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=5.0,  # ~5% failure rate
                        min_value=0.0,
                        max_value=20.0,
                        seasonality_type="peak_offpeak",
                        peak_start_hour=8,
                        peak_end_hour=22,
                        seasonality_strength=0.35,
                        noise_level=0.2,
                        spike_probability=0.02,
                        spike_magnitude=0.3,
                        interval_minutes=15,
                        seed=211,
                    ),
                ),
            ),
            Column(
                name="RRC_ConnEstabSucc",
                data_type="int64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=95.0,
                        min_value=55.0,
                        max_value=145.0,
                        seasonality_type="peak_offpeak",
                        peak_start_hour=8,
                        peak_end_hour=22,
                        seasonality_strength=0.35,
                        noise_level=0.2,
                        spike_probability=0.01,
                        spike_magnitude=0.25,
                        interval_minutes=15,
                        seed=207,
                    ),
                ),
            ),
            Column(
                name="RRC_ConnEstabAtt",
                data_type="int64",
                column_type=ColumnType.DERIVED,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                derivation=Derivation(
                    function_type=DerivationFunctionType.SUM,
                    dependent_columns=["RRC_ConnEstabSucc", "RRC_ConnEstabFail"],
                    params=SumParams(),
                ),
            ),
            Column(
                name="ERAB_EstabInitSuccNbr_QCI",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=90.0,
                        min_value=50.0,
                        max_value=140.0,
                        seasonality_type="peak_offpeak",
                        peak_start_hour=8,
                        peak_end_hour=22,
                        seasonality_strength=0.35,
                        noise_level=0.2,
                        spike_probability=0.01,
                        spike_magnitude=0.25,
                        interval_minutes=15,
                        seed=208,
                    ),
                ),
            ),
            Column(
                name="DL_PRB_Utilization",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=50.0,
                        min_value=20.0,
                        max_value=85.0,
                        seasonality_type="peak_offpeak",
                        peak_start_hour=8,
                        peak_end_hour=22,
                        seasonality_strength=0.4,
                        noise_level=0.18,
                        spike_probability=0.02,
                        spike_magnitude=0.2,
                        interval_minutes=15,
                        seed=209,
                    ),
                ),
            ),
            Column(
                name="Cell_Availability",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=99.5,
                        min_value=97.0,
                        max_value=100.0,
                        seasonality_type="none",
                        noise_level=0.05,
                        spike_probability=0.005,
                        spike_magnitude=0.5,
                        interval_minutes=15,
                        seed=210,
                    ),
                ),
            ),
        ],
    )

    # ENTITY RELATIONSHIPS
    relationships = [
        # transport_link -> cell_site (one-to-many)
        # Cells connect to transport network via specific device/interface pairs
        EntityRelationship(
            parent_entity="transport_link",
            child_entity="cell_site",
            relationship_type=EntityRelationshipType.ONE_TO_MANY,
            join_columns={
                "Device_ID": "Transport_Device_ID",
                "Interface_ID": "Transport_Interface_ID",
            },
        ),
    ]

    # GLOBAL TIMESTAMP CONFIGURATION
    global_timestamp = GlobalTimestamp(
        t_start=global_start_time,
        t_end=global_end_time,
        time_interval=global_time_interval,
    )

    return DataSchema(
        entities=[transport_link, core_node, cell_site],
        entity_relationships=relationships,
        global_timestamp=global_timestamp,
    )


def inject_incident(
    df: pd.DataFrame,
    metric_shifts: dict,
    incident_start: pd.Timestamp,
    incident_end: pd.Timestamp,
    entity_col: str = None,
    affected_entity_ids=None,
    entity_mask: pd.Series = None,
    seed: int = 42,
) -> pd.DataFrame:
    """Apply a sustained metric shift with noise during an incident window.

    metric_shifts maps column names to dicts with:
      'shift': additive offset to apply
      'noise_std': standard deviation of Gaussian noise added on top of shift
      'clip': (min, max) tuple to clip the result

    Affected rows are selected by one of:
      entity_mask: precomputed boolean Series (e.g. for compound entity keys)
      entity_col + affected_entity_ids: rows where entity_col is in affected_entity_ids
      neither: all rows
    """
    rng = np.random.default_rng(seed)
    df = df.copy()
    ts = pd.to_datetime(df["Timestamp"])

    # Get affected data slice
    time_mask = (ts >= incident_start) & (ts <= incident_end)
    if entity_mask is not None:
        ent_mask = entity_mask.reindex(df.index, fill_value=False)
    elif affected_entity_ids is not None:
        ent_mask = df[entity_col].isin(affected_entity_ids)
    else:
        ent_mask = pd.Series(True, index=df.index)
    affected = time_mask & ent_mask

    # Modify metrics according to incident spec
    n = int(affected.sum())
    for col, spec in metric_shifts.items():
        original_dtype = df[col].dtype

        shift = spec.get("shift", 0)
        noise_std = spec.get("noise_std", 0.0)
        lo, hi = spec.get("clip", (None, None))

        perturbation = shift + (rng.normal(0, noise_std, n) if noise_std > 0 else 0)

        df.loc[affected, col] = df.loc[affected, col] + perturbation
        df.loc[affected, col] = df.loc[affected, col].clip(lo, hi)
        if pd.api.types.is_integer_dtype(original_dtype):
            df[col] = df[col].round().astype(original_dtype)

    return df


async def main():
    # Generate the full base dataset for the entire time window in one pass.
    schema = get_telecom_ran_schema(
        global_start_time="2026-01-01T00:00:00Z",
        global_end_time="2026-01-03T00:00:00Z",
        global_time_interval="15min",
    )

    async with rf.Connection.from_env() as conn:
        action = ra.GenerateFromDataSchema(schema=schema)
        builder = rf.WorkflowBuilder()
        builder.add(action)
        workflow = await builder.start(conn)
        print(f"Workflow ID: {workflow.id()}")
        datasets = await workflow.datasets().collect()
        print(f"Generated {len(datasets)} datasets")

        transport_link_df = None
        core_node_df = None
        cell_site_df = None
        for remote_ds in datasets:
            ds = await remote_ds.to_local(conn)
            if ds.name() == "transport_link":
                transport_link_df = ds.to_pandas()
            elif ds.name() == "core_node":
                core_node_df = ds.to_pandas()
            elif ds.name() == "cell_site":
                cell_site_df = ds.to_pandas()

    # Save base data
    transport_link_df.to_csv("transport_link_data.csv", index=False)
    core_node_df.to_csv("core_node_data.csv", index=False)
    cell_site_df.to_csv("cell_site_data.csv", index=False)
    print("Saved 3 base datasets")

    # Inject incident: RTR_001/eth0 transport link degrades, cascading to connected
    # cell sites and causing a proportional but modest impact on core nodes
    print("Injecting incident in cells, nodes, and links...")
    incident_start = pd.Timestamp("2026-01-02T00:00:00Z")
    incident_end = pd.Timestamp("2026-01-02T06:00:00Z")

    # Transport link: RTR_001/eth0 shows packet loss, latency, and jitter spikes
    # as the root cause of the incident
    link_mask = (transport_link_df["Device_ID"] == "RTR_001") & (
        transport_link_df["Interface_ID"] == "eth0"
    )
    transport_link_inc_df = inject_incident(
        transport_link_df,
        metric_shifts={
            "Packet_Loss_Percent": {"shift": 1.2, "noise_std": 0.3, "clip": (0.0, 2.0)},
            "Latency_ms": {"shift": 8.0, "noise_std": 2.0, "clip": (8.0, 25.0)},
            "Jitter_ms": {"shift": 1.5, "noise_std": 0.5, "clip": (0.5, 5.0)},
        },
        incident_start=incident_start,
        incident_end=incident_end,
        entity_mask=link_mask,
        seed=502,
    )

    # Cell sites: only those connected to RTR_001/eth0 degrade
    # RRC connection failures increase and cell availability drops due to transport issues
    affected_cell_ids = cell_site_df.loc[
        (cell_site_df["Transport_Device_ID"] == "RTR_001")
        & (cell_site_df["Transport_Interface_ID"] == "eth0"),
        "Cell_ID",
    ].unique()
    cell_site_inc_df = inject_incident(
        cell_site_df,
        metric_shifts={
            "RRC_ConnEstabFail": {"shift": 10, "noise_std": 3, "clip": (10, 30)},
            "Cell_Availability": {"shift": -2.5, "noise_std": 0.5, "clip": (94, 100)},
        },
        incident_start=incident_start,
        incident_end=incident_end,
        entity_col="Cell_ID",
        affected_entity_ids=list(affected_cell_ids),
        seed=501,
    )
    # Recompute derived column after modifying RRC_ConnEstabFail.
    cell_site_inc_df["RRC_ConnEstabAtt"] = (
        cell_site_inc_df["RRC_ConnEstabSucc"] + cell_site_inc_df["RRC_ConnEstabFail"]
    )

    # Core nodes: modest proportional impact from UEs on affected cells failing to
    # connect, causing a small uptick in signaling load and a slight UE count drop.
    core_node_inc_df = inject_incident(
        core_node_df,
        metric_shifts={
            "MM_AttachedUEs": {"shift": -200, "noise_std": 75, "clip": (3000, 7500)},
            "CPU_Load": {"shift": 5, "noise_std": 2, "clip": (30, 90)},
        },
        incident_start=incident_start,
        incident_end=incident_end,
        seed=500,
    )

    transport_link_inc_df.to_csv("transport_link_with_inc_data.csv", index=False)
    core_node_inc_df.to_csv("core_node_with_inc_data.csv", index=False)
    cell_site_inc_df.to_csv("cell_site_with_inc_data.csv", index=False)
    print("Saved 3 incident datasets")


asyncio.run(main())
