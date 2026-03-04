import asyncio

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
    GlobalTimestamp,
    IDParams,
    MapValuesParams,
    StateMachineParams,
    TimeseriesParams,
    Timestamp,
    Transition,
)

load_dotenv()


def build_state_machine_params(transitions) -> StateMachineParams:
    """Construct a StateMachineParams object for an IoT device."""
    return StateMachineParams(
        column_name="device_state",
        trigger_column_name="event_type",
        initial_state="operational",
        states=["operational", "warning", "critical", "maintenance", "offline"],
        terminal_states=["offline"],
        transitions=transitions,
    )


def get_device_entity(
    name,
    cardinality,
    id_prefix,
    device_type_value,
    state_machine_params,
    location_seed,
    firmware_seed,
    battery_seed,
    signal_seed,
):
    """Build an IOT device entity with the given sensor-specific parameters."""
    return Entity(
        name=name,
        cardinality=cardinality,
        timestamp=Timestamp(column_name="timestamp", data_type="timestamp"),
        columns=[
            Column(
                name="device_id",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.ID,
                    params=IDParams(template_str=f"{id_prefix}_{{id}}"),
                ),
            ),
            Column(
                name="device_type",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=[device_type_value],
                        with_replacement=True,
                    ),
                ),
            ),
            Column(
                name="location_zone",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=["zone_A", "zone_B", "zone_C", "zone_D"],
                        with_replacement=True,
                        seed=location_seed,
                    ),
                ),
            ),
            Column(
                name="firmware_version",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=["v1.0", "v1.1", "v2.0"],
                        weights=[0.3, 0.4, 0.3],
                        with_replacement=True,
                        seed=firmware_seed,
                    ),
                ),
            ),
            Column(
                name="device_state",
                data_type="string",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.STATE_MACHINE,
                    params=state_machine_params,
                ),
            ),
            Column(
                name="is_critical",
                data_type="int64",
                column_type=ColumnType.DERIVED,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                derivation=Derivation(
                    function_type=DerivationFunctionType.MAP_VALUES,
                    dependent_columns=["device_state"],
                    params=MapValuesParams(
                        mapping=[{"from": "critical", "to": 1}],
                        default=0,
                    ),
                ),
            ),
            Column(
                name="battery_level",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=70.0,
                        min_value=10.0,
                        max_value=100.0,
                        seasonality_type="none",
                        noise_level=0.05,
                        spike_probability=0.01,
                        spike_magnitude=0.3,
                        interval_minutes=15,
                        seed=battery_seed,
                    ),
                ),
            ),
            Column(
                name="signal_strength",
                data_type="float64",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.TIMESERIES,
                    params=TimeseriesParams(
                        base_value=75.0,
                        min_value=20.0,
                        max_value=100.0,
                        seasonality_type="none",
                        noise_level=0.15,
                        spike_probability=0.03,
                        spike_magnitude=0.4,
                        interval_minutes=15,
                        seed=signal_seed,
                    ),
                ),
            ),
        ],
    )


def get_iot_temp_schema(
    n_devices=200,
    global_start_time="2026-01-01T00:00:00Z",
    global_end_time="2026-01-02T00:00:00Z",
    global_time_interval="15min",
) -> DataSchema:
    """Create the IOT device schema for temperature sensors.

    More volatile: higher threshold exceedance rate, slower warning recovery.

    State transitions:

    operational --[reading_recorded, 0.94]----> operational  (self-loop)
    operational --[threshold_exceeded, 0.06]--> warning

    warning     --[reading_recorded, 0.35]----> operational
    warning     --[threshold_exceeded, 0.25]--> critical
    warning     --[maintenance_required, 0.40]-> maintenance

    critical    --[threshold_exceeded, 0.87]--> critical     (self-loop, ~2hr avg)
    critical    --[maintenance_required, 0.11]-> maintenance
    critical    --[reading_recorded, 0.02]-----> offline      (terminal)

    maintenance --[issue_resolved, 0.92]------> operational
    maintenance --[reading_recorded, 0.08]----> offline      (terminal)
    """
    state_machine_params = build_state_machine_params(
        [
            Transition(
                trigger="reading_recorded",
                source="operational",
                dest="operational",
                probability=0.94,
            ),
            Transition(
                trigger="threshold_exceeded",
                source="operational",
                dest="warning",
                probability=0.06,
            ),
            Transition(
                trigger="reading_recorded",
                source="warning",
                dest="operational",
                probability=0.35,
            ),
            Transition(
                trigger="threshold_exceeded",
                source="warning",
                dest="critical",
                probability=0.25,
            ),
            Transition(
                trigger="maintenance_required",
                source="warning",
                dest="maintenance",
                probability=0.40,
            ),
            Transition(
                trigger="threshold_exceeded",
                source="critical",
                dest="critical",
                probability=0.87,
            ),
            Transition(
                trigger="maintenance_required",
                source="critical",
                dest="maintenance",
                probability=0.11,
            ),
            Transition(
                trigger="reading_recorded",
                source="critical",
                dest="offline",
                probability=0.02,
            ),
            Transition(
                trigger="issue_resolved",
                source="maintenance",
                dest="operational",
                probability=0.92,
            ),
            Transition(
                trigger="reading_recorded",
                source="maintenance",
                dest="offline",
                probability=0.08,
            ),
        ]
    )

    device = get_device_entity(
        name="device",
        cardinality=n_devices,
        id_prefix="TEMP",
        device_type_value="temperature_sensor",
        state_machine_params=state_machine_params,
        location_seed=1002,
        firmware_seed=1003,
        battery_seed=1004,
        signal_seed=1005,
    )

    global_timestamp = GlobalTimestamp(
        t_start=global_start_time,
        t_end=global_end_time,
        time_interval=global_time_interval,
    )

    return DataSchema(
        entities=[device],
        entity_relationships=[],
        global_timestamp=global_timestamp,
    )


def get_iot_hum_schema(
    n_devices=200,
    global_start_time="2026-01-01T00:00:00Z",
    global_end_time="2026-01-02T00:00:00Z",
    global_time_interval="15min",
) -> DataSchema:
    """Create the IOT device schema for humidity sensors.

    More self-resolving: lower threshold exceedance rate, faster warning recovery.

    State transitions:

    operational --[reading_recorded, 0.96]----> operational  (self-loop)
    operational --[threshold_exceeded, 0.04]--> warning

    warning     --[reading_recorded, 0.55]----> operational
    warning     --[threshold_exceeded, 0.15]--> critical
    warning     --[maintenance_required, 0.30]-> maintenance

    critical    --[threshold_exceeded, 0.82]--> critical     (self-loop, ~1.5hr avg)
    critical    --[maintenance_required, 0.16]-> maintenance
    critical    --[reading_recorded, 0.02]-----> offline      (terminal)

    maintenance --[issue_resolved, 0.94]------> operational
    maintenance --[reading_recorded, 0.06]----> offline      (terminal)
    """
    state_machine_params = build_state_machine_params(
        [
            Transition(
                trigger="reading_recorded",
                source="operational",
                dest="operational",
                probability=0.96,
            ),
            Transition(
                trigger="threshold_exceeded",
                source="operational",
                dest="warning",
                probability=0.04,
            ),
            Transition(
                trigger="reading_recorded",
                source="warning",
                dest="operational",
                probability=0.55,
            ),
            Transition(
                trigger="threshold_exceeded",
                source="warning",
                dest="critical",
                probability=0.15,
            ),
            Transition(
                trigger="maintenance_required",
                source="warning",
                dest="maintenance",
                probability=0.30,
            ),
            Transition(
                trigger="threshold_exceeded",
                source="critical",
                dest="critical",
                probability=0.82,
            ),
            Transition(
                trigger="maintenance_required",
                source="critical",
                dest="maintenance",
                probability=0.16,
            ),
            Transition(
                trigger="reading_recorded",
                source="critical",
                dest="offline",
                probability=0.02,
            ),
            Transition(
                trigger="issue_resolved",
                source="maintenance",
                dest="operational",
                probability=0.94,
            ),
            Transition(
                trigger="reading_recorded",
                source="maintenance",
                dest="offline",
                probability=0.06,
            ),
        ]
    )

    device = get_device_entity(
        name="device",
        cardinality=n_devices,
        id_prefix="HUM",
        device_type_value="humidity_sensor",
        state_machine_params=state_machine_params,
        location_seed=2002,
        firmware_seed=2003,
        battery_seed=2004,
        signal_seed=2005,
    )

    global_timestamp = GlobalTimestamp(
        t_start=global_start_time,
        t_end=global_end_time,
        time_interval=global_time_interval,
    )

    return DataSchema(
        entities=[device],
        entity_relationships=[],
        global_timestamp=global_timestamp,
    )


def get_iot_pres_schema(
    n_devices=200,
    global_start_time="2026-01-01T00:00:00Z",
    global_end_time="2026-01-02T00:00:00Z",
    global_time_interval="15min",
) -> DataSchema:
    """Create the IOT device schema for pressure sensors.

    Most stable: lowest threshold exceedance rate, but hardest to fix once critical.

    State transitions:

    operational --[reading_recorded, 0.97]----> operational  (self-loop)
    operational --[threshold_exceeded, 0.03]--> warning

    warning     --[reading_recorded, 0.25]----> operational
    warning     --[threshold_exceeded, 0.35]--> critical
    warning     --[maintenance_required, 0.40]-> maintenance

    critical    --[threshold_exceeded, 0.92]--> critical     (self-loop, ~3hr avg)
    critical    --[maintenance_required, 0.05]-> maintenance
    critical    --[reading_recorded, 0.03]-----> offline      (terminal)

    maintenance --[issue_resolved, 0.88]------> operational
    maintenance --[reading_recorded, 0.12]----> offline      (terminal)
    """
    state_machine_params = build_state_machine_params(
        [
            Transition(
                trigger="reading_recorded",
                source="operational",
                dest="operational",
                probability=0.97,
            ),
            Transition(
                trigger="threshold_exceeded",
                source="operational",
                dest="warning",
                probability=0.03,
            ),
            Transition(
                trigger="reading_recorded",
                source="warning",
                dest="operational",
                probability=0.25,
            ),
            Transition(
                trigger="threshold_exceeded",
                source="warning",
                dest="critical",
                probability=0.35,
            ),
            Transition(
                trigger="maintenance_required",
                source="warning",
                dest="maintenance",
                probability=0.40,
            ),
            Transition(
                trigger="threshold_exceeded",
                source="critical",
                dest="critical",
                probability=0.92,
            ),
            Transition(
                trigger="maintenance_required",
                source="critical",
                dest="maintenance",
                probability=0.05,
            ),
            Transition(
                trigger="reading_recorded",
                source="critical",
                dest="offline",
                probability=0.03,
            ),
            Transition(
                trigger="issue_resolved",
                source="maintenance",
                dest="operational",
                probability=0.88,
            ),
            Transition(
                trigger="reading_recorded",
                source="maintenance",
                dest="offline",
                probability=0.12,
            ),
        ]
    )

    device = get_device_entity(
        name="device",
        cardinality=n_devices,
        id_prefix="PRES",
        device_type_value="pressure_sensor",
        state_machine_params=state_machine_params,
        location_seed=3002,
        firmware_seed=3003,
        battery_seed=3004,
        signal_seed=3005,
    )

    global_timestamp = GlobalTimestamp(
        t_start=global_start_time,
        t_end=global_end_time,
        time_interval=global_time_interval,
    )

    return DataSchema(
        entities=[device],
        entity_relationships=[],
        global_timestamp=global_timestamp,
    )


async def main():
    schemas = [get_iot_temp_schema, get_iot_pres_schema, get_iot_hum_schema]
    iot_df_list = []
    for sc_func in schemas:
        schema = sc_func()

        action = ra.GenerateFromDataSchema(
            schema=schema,
        )

        builder = rf.WorkflowBuilder()
        builder.add(action)

        async with rf.Connection.from_env() as conn:
            workflow = await builder.start(conn)
            print(f"Workflow ID: {workflow.id()}")

            remote_dataset = await workflow.datasets().nth(0)
            dataset = await remote_dataset.to_local(conn)
            print(f"Generated dataset with {dataset.table.num_rows} rows...")
            iot_df_list.append(dataset.to_pandas())

    all_iot_df = pd.concat(iot_df_list)
    all_iot_df.to_csv("iot_device_data.csv", index=False)


asyncio.run(main())
