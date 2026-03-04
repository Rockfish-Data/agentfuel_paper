import asyncio

import rockfish as rf
import rockfish.actions as ra
from dotenv import load_dotenv
from rockfish.actions.ent import (
    DataSchema,
    Entity,
    CategoricalParams,
    Column,
    ColumnType,
    ColumnCategoryType,
    Domain,
    DomainType,
    IDParams,
    MapValuesParams,
    NormalDistParams,
    Timestamp,
    GlobalTimestamp,
    Derivation,
    DerivationFunctionType,
    SampleFromColumnParams,
    StateMachineParams,
    Transition,
    EntityRelationship,
    EntityRelationshipType,
)

load_dotenv()


def get_ecommerce_schema(
    n_users=100,
    n_sessions=500,
    global_start_time="2026-01-01T00:00:00Z",
    global_end_time="2026-01-03T00:00:00Z",
    global_time_interval="1min",
) -> DataSchema:
    """Create the ecommerce schema."""
    # ENTITY 1: users
    users = Entity(
        name="users",
        cardinality=n_users,
        columns=[
            Column(
                name="user_id",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.ID, params=IDParams(template_str="USER_{id}")
                ),
            ),
            Column(
                name="age",
                data_type="int64",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.NORMAL_DIST,
                    params=NormalDistParams(mean=35.0, std=10.0),
                ),
            ),
            Column(
                name="country",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=["IND", "SG", "US"], with_replacement=True
                    ),
                ),
            ),
            Column(
                name="subscription_type",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=["free", "premium"],
                        weights=[0.6, 0.4],
                        with_replacement=True,
                    ),
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
                        values=["mobile", "desktop"], with_replacement=True
                    ),
                ),
            ),
        ],
    )

    # ENTITY 2: sessions
    sessions = Entity(
        name="sessions",
        cardinality=n_sessions,
        timestamp=Timestamp(column_name="timestamp"),
        columns=[
            Column(
                name="session_id",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.ID, params=IDParams(template_str="SESSION_{id}")
                ),
            ),
            Column(
                name="user_id",
                data_type="string",
                column_type=ColumnType.DERIVED,
                column_category_type=ColumnCategoryType.METADATA,
                derivation=Derivation(
                    function_type=DerivationFunctionType.SAMPLE_FROM_COLUMN,
                    dependent_columns=["users.user_id"],
                    params=SampleFromColumnParams(with_replacement=True, seed=42),
                ),
            ),
            Column(
                name="referrer",
                data_type="string",
                column_type=ColumnType.INDEPENDENT,
                column_category_type=ColumnCategoryType.METADATA,
                domain=Domain(
                    type=DomainType.CATEGORICAL,
                    params=CategoricalParams(
                        values=["direct", "search", "social", "email"],
                        with_replacement=True,
                    ),
                ),
            ),
            # homepage  --[browse, 0.4]----------->  search
            # homepage  --[view_product, 0.1]----->  product
            # homepage  --[leave, 0.5]------------>  exit
            #
            # search    --[view_product, 0.7]----->  product
            #
            # product   --[add_to_cart, 0.1]------>  cart
            # product   --[back, 0.6]------------->  search
            #
            # cart      --[continue_shopping, 0.3]->  product
            # cart      --[cart_abandoned, 0.4]--->  exit
            # cart      --[checkout, 0.3]--------->  checkout
            #
            # checkout  --[complete, 0.7]--------->  purchase  (terminal)
            # checkout  --[leave, 0.3]------------>  exit      (terminal)
            Column(
                name="page",
                data_type="string",
                column_type=ColumnType.STATEFUL,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                domain=Domain(
                    type=DomainType.STATE_MACHINE,
                    params=StateMachineParams(
                        column_name="page",
                        trigger_column_name="action",
                        initial_state="homepage",
                        states=[
                            "homepage",
                            "search",
                            "product",
                            "cart",
                            "checkout",
                            "purchase",
                            "exit",
                        ],
                        terminal_states=["purchase", "exit"],
                        transitions=[
                            Transition(
                                trigger="browse",
                                source="homepage",
                                dest="search",
                                probability=0.4,
                            ),
                            Transition(
                                trigger="view_product",
                                source="homepage",
                                dest="product",
                                probability=0.1,
                            ),
                            Transition(
                                trigger="leave",
                                source="homepage",
                                dest="exit",
                                probability=0.5,
                            ),
                            Transition(
                                trigger="add_to_cart",
                                source="product",
                                dest="cart",
                                probability=0.1,
                            ),
                            Transition(
                                trigger="back",
                                source="product",
                                dest="search",
                                probability=0.6,
                            ),
                            Transition(
                                trigger="view_product",
                                source="search",
                                dest="product",
                                probability=0.7,
                            ),
                            Transition(
                                trigger="continue_shopping",
                                source="cart",
                                dest="product",
                                probability=0.3,
                            ),
                            Transition(
                                trigger="cart_abandoned",
                                source="cart",
                                dest="exit",
                                probability=0.4,
                            ),
                            Transition(
                                trigger="checkout",
                                source="cart",
                                dest="checkout",
                                probability=0.3,
                            ),
                            Transition(
                                trigger="complete",
                                source="checkout",
                                dest="purchase",
                                probability=0.7,
                            ),
                            Transition(
                                trigger="leave",
                                source="checkout",
                                dest="exit",
                                probability=0.3,
                            ),
                        ],
                    ),
                ),
            ),
            Column(
                name="did_purchase",
                data_type="int64",
                column_type=ColumnType.DERIVED,
                column_category_type=ColumnCategoryType.MEASUREMENT,
                derivation=Derivation(
                    function_type=DerivationFunctionType.MAP_VALUES,
                    dependent_columns=["page"],
                    params=MapValuesParams(
                        mapping=[
                            {"from": "purchase", "to": 1},
                        ],
                        default=0,
                    ),
                ),
            ),
        ],
    )

    # ENTITY RELATIONSHIPS
    relationships = [
        # users -> sessions (one-to-many)
        # Users can have multiple browsing sessions
        EntityRelationship(
            parent_entity="users",
            child_entity="sessions",
            relationship_type=EntityRelationshipType.ONE_TO_MANY,
            join_columns={
                "user_id": "user_id",
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
        entities=[users, sessions],
        entity_relationships=relationships,
        global_timestamp=global_timestamp,
    )


async def main():
    schema = get_ecommerce_schema()

    action = ra.GenerateFromDataSchema(
        schema=schema,
    )

    builder = rf.WorkflowBuilder()
    builder.add(action)

    async with rf.Connection.from_env() as conn:
        workflow = await builder.start(conn)
        print(f"Workflow ID: {workflow.id()}")

        datasets = await workflow.datasets().collect()
        print(f"Generated {len(datasets)} datasets")

        users_dataset = None
        sessions_dataset = None
        for remote_ds in datasets:
            ds = await remote_ds.to_local(conn)
            if ds.name() == "users":
                users_dataset = ds
            elif ds.name() == "sessions":
                sessions_dataset = ds

        users_df = users_dataset.to_pandas()
        sessions_df = sessions_dataset.to_pandas()

        users_df.to_csv("ecommerce_users_data.csv", index=False)
        sessions_df.to_csv("ecommerce_sessions_data.csv", index=False)


asyncio.run(main())
