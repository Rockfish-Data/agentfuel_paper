# AgentFuel Benchmarks

Datasets and query sets for the three benchmark settings from the "Generating Expressive and Customizable Evals for Timeseries Data Analysis Agents with AgentFuel" paper.

All datasets and query sets were generated using AgentFuel's data generation and question-answer generation modules.

### E-commerce
Product analytics for an e-commerce website. Browsing sessions are generated using a state machine covering browsing flows, cart abandonment, and purchase flows.

Datasets: `ecommerce_users_data.csv`, `ecommerce_sessions_data.csv`

Queries: 12 stateless (`ecommerce_basic.csv`) + 12 stateful (`ecommerce_stateful.csv`)

### IoT
IoT device monitoring with three sensor exemplars: temperature, pressure, and humidity, each with its own operations state machine and device health metrics.

Dataset: `iot_device_data.csv`

Queries: 12 stateless (`iot_basic.csv`) + 12 stateful (`iot_stateful.csv`)

### Telecom
Telecommunications network telemetry across three related entities: cell sites, transport links, and core nodes. The `_with_inc_` dataset variants include an injected cascading incident: a transport link degrades (elevated packet loss, latency, jitter), cascading to connected cell sites (higher RRC failures, lower availability), with a modest effect on core nodes (reduced attached UEs, increased CPU load).

Datasets: `cell_site_data.csv`, `transport_link_data.csv`, `core_node_data.csv` (and `_with_inc_` variants)

Queries: 12 stateless (`telecom_basic.csv`) + 12 incident-specific (`telecom_incident.csv`)