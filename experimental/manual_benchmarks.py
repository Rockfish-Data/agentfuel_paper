from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "orig_data"

BENCHMARKS = [
    {
        "name": "ecommerce_stateful",
        "query_type": "stateful",
        "data_files": [
            {"path": DATA_DIR / "ecommerce_users_data.csv", "name": "users"},
            {"path": DATA_DIR / "ecommerce_sessions_data.csv", "name": "sessions"},
        ],
        "questions": [
            {
                "template": "Golden Query",
                "query": "How many users added more than 4 items within 1 hour to the cart but exited without purchasing?",
                "correct_answer": "0 users",
            },
            {
                "template": "Event Existence Check",
                "query": "Did premium users make purchases?",
                "correct_answer": "15 users",
            },
            {
                "template": "Time Between Events",
                "query": "What is the average time between viewing a product and adding it to cart?",
                "correct_answer": "13.7 mins",
            },
            {
                "template": "Event Sequence Matching",
                "query": "Did users view, add to cart, checkout, and then purchase?",
                "correct_answer": "63 sessions",
            },
            {
                "template": "Count Events After Trigger",
                "query": "How many product views after cart abandonment?",
                "correct_answer": "0 views",
            },
            {
                "template": "Conditional Event Detection",
                "query": "Did any users add to cart but not checkout within 5 minutes?",
                "correct_answer": "78 users",
            },
            {
                "template": "Threshold Crossing Without Outcome",
                "query": "Did any users view products more than 3 times without adding to cart?",
                "correct_answer": "0 users",
            },
            {
                "template": "State Reached Analysis",
                "query": "Which users reached the checkout stage, and where were these users acquired from?",
                "correct_answer": "56 unique users, top 3 referrers: email, social, direct",
            },
            {
                "template": "Count Events In State",
                "query": "How many product views occurred while users had an item in their cart?",
                "correct_answer": "599 views",
            },
            {
                "template": "State Duration Analysis",
                "query": "How long on average do users spend with items in cart before purchasing or abandoning?",
                "correct_answer": "Purchasing: 7.7 mins, Abandoning: 8.3 mins",
            },
            {
                "template": "State Transition Analysis",
                "query": "What are the common state transitions from browsing to purchase or abandonment?",
                "correct_answer": "Top 3: search -> product (2170), product -> search (1997), product -> cart (329)",
            },
            {
                "template": "Multi-State Occupancy Analysis",
                "query": "What percentage of time do users spend in each page?",
                "correct_answer": "product=42.97%, search=40.09%, homepage=9.24%, cart=6.08%, checkout=1.63%",
            },
        ],
    },
    {
        "name": "iot_stateful",
        "query_type": "stateful",
        "data_files": [
            {"path": DATA_DIR / "iot_device_data.csv", "name": "iot_devices"},
        ],
        "questions": [
            {
                "template": "Golden Query",
                "query": "How many sensors exceeded a warning threshold more than 3 times in 12 hours without triggering maintenance?",
                "correct_answer": "112 sensors",
            },
            {
                "template": "Event Existence Check",
                "query": "Did sensors on firmware v1.0 ever reach critical state?",
                "correct_answer": "96 sensors",
            },
            {
                "template": "Time Between Events",
                "query": "What is the average time between threshold exceeded and maintenance required?",
                "correct_answer": "2 hr 10 mins",
            },
            {
                "template": "Event Sequence Matching",
                "query": "Were any v2.0 sensors in warning, critical, maintenance, and then operational?",
                "correct_answer": "59 sensors",
            },
            {
                "template": "Count Events After Trigger",
                "query": "How many readings were recorded after maintenance was required?",
                "correct_answer": "21937 readings",
            },
            {
                "template": "Conditional Event Detection",
                "query": "Did any sensors go from warning to critical without maintenance within an hour?",
                "correct_answer": "324 sensors",
            },
            {
                "template": "Threshold Crossing Without Outcome",
                "query": "Did any v1.1 sensors raise warnings more than 3 times without ever reaching a critical status?",
                "correct_answer": "35 sensors",
            },
            {
                "template": "State Reached Analysis",
                "query": "Which sensors reached critical status, and what location zone were they in?",
                "correct_answer": "324 sensors: zone_A=88, zone_C=81, zone_B=80, zone_D=75",
            },
            {
                "template": "Count Events In State",
                "query": "How many readings were recorded while sensors were being maintained? Show me a breakdown by device type.",
                "correct_answer": "68 readings: humidity_sensor=9, pressure_sensor=27, temperature_sensor=32",
            },
            {
                "template": "State Duration Analysis",
                "query": "How long on average do devices stay in critical status before entering maintenance? Show me a breakdown by device type.",
                "correct_answer": "humidity_sensor=1 hr 18 min 52 sec, pressure_sensor=2 hr 57 min 32 sec, temperature_sensor=1 hr 49 min 8 sec",
            },
            {
                "template": "State Transition Analysis",
                "query": "What are the most common state transitions from degraded status to operational or offline? Show me the results according to the device location.",
                "correct_answer": "zone_A: warning->operational=204, critical->offline=26; zone_B: warning->operational=214, critical->offline=19; zone_C: warning->operational=202, critical->offline=22; zone_D: warning->operational=154, critical->offline=25",
            },
            {
                "template": "Multi-State Occupancy Analysis",
                "query": "What percentage of time do devices spend in each status? Show me a breakdown by device type.",
                "correct_answer": "humidity_sensor: operational=91.40%, warning=3.82%, critical=3.12%, maintenance=1.66%; pressure_sensor: operational=87.16%, critical=8.69%, warning=2.63%, maintenance=1.53%; temperature_sensor: operational=82.28%, critical=9.51%, warning=5.18%, maintenance=3.03%",
            },
        ],
    },
    {
        "name": "telecom_incident",
        "query_type": "incident",
        "data_files": [
            {"path": DATA_DIR / "cell_site_with_inc_data.csv", "name": "cell_sites"},
            {"path": DATA_DIR / "core_node_with_inc_data.csv", "name": "core_nodes"},
            {
                "path": DATA_DIR / "transport_link_with_inc_data.csv",
                "name": "transport_links",
            },
        ],
        "questions": [
            {
                "template": "incident_existence, transport_link, cov_1",
                "query": "Did the router that caused the outage have elevated packet loss on January 2 morning?",
                "correct_answer": "Yes, 3.2x more packet loss",
            },
            {
                "template": "kpi_aggregation, transport_link, cov_1",
                "query": "What was the average latency on the affected router link during the incident?",
                "correct_answer": "19.69 ms",
            },
            {
                "template": "entity_count, transport_link, cov_1",
                "query": "How many transport links were experiencing high packet loss during the outage?",
                "correct_answer": "1, Device_ID=RTR_001, Interface_ID=eth0",
            },
            {
                "template": "entity_top_k, transport_link, cov_1",
                "query": "Which transport link had the worst latency during the outage?",
                "correct_answer": "Device_ID=RTR_001, Interface_ID=eth0",
            },
            {
                "template": "incident_existence, cell_site, cov_1",
                "query": "Were connection failures elevated on cells behind the affected router during the outage?",
                "correct_answer": "Yes, 3.7x more failures",
            },
            {
                "template": "kpi_aggregation, cell_site, cov_1",
                "query": "What was the average connection failure rate on cells behind the affected router during the outage?",
                "correct_answer": "13.84",
            },
            {
                "template": "entity_count, cell_site, cov_1",
                "query": "How many cells lost availability during the outage on January 2?",
                "correct_answer": "19",
            },
            {
                "template": "entity_top_k, cell_site, cov_1",
                "query": "Which 5 cells had the most connection failures during the outage?",
                "correct_answer": "CELL_62, CELL_67, CELL_28, CELL_45, CELL_80",
            },
            {
                "template": "incident_existence, cell_site cov_1 + core_node cov_1",
                "query": "Were connection failures elevated on cells behind the affected router during the outage, while the core nodes were also under load?",
                "correct_answer": "Yes, cell connection failures were 3.7x higher, core node CPU load was 1.1x higher",
            },
            {
                "template": "incident_existence, cell_site cov_1 + core_node cov_2",
                "query": "Were connection failures elevated on cells behind the affected router during the outage, even though core node session counts looked normal?",
                "correct_answer": "Yes, cell connection failures were 3.7x higher",
            },
            {
                "template": "entity_count, cell_site cov_1 + core_node cov_1",
                "query": "How many cells lost availability during the outage on January 2, while the core nodes were also under load?",
                "correct_answer": "19",
            },
            {
                "template": "entity_count, cell_site cov_1 + core_node cov_5",
                "query": "How many cells lost availability during the outage on January 2, compared to the same time the day before when core nodes were healthy?",
                "correct_answer": "19",
            },
        ],
    },
]
