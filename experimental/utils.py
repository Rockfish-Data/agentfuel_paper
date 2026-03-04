import re

ANOMALY_WORDS = [
    "anomaly",
    "abnormal",
    "unusual",
    "unexpected",
    "spike",
    "deviation",
    "outlier",
]

ORDER_WORDS = [
    "trend",
    "over time",
    "previous",
    "consecutive",
    "rolling average",
    "cumulative",
    "running",
]


def contains_any(text, keywords):
    return any(k in text for k in keywords)


def classify_using_keywords(query: str) -> str:
    q = query.lower()

    if contains_any(q, ANOMALY_WORDS):
        return "Incident"

    if contains_any(q, ORDER_WORDS):
        return "Stateful_No_Incident"

    return "Stateless"


ORDER_STATEFUL_FUNCS = r"\b(lag|lead)\s*\("
RUNNING_TERMS = r"\b(cumulative|running|moving|rolling)\b"

ANOMALY_TERMS = r"\b(stddev|stddev_pop|variance|var_pop|var_samp)\b"
AVG_COMPARE = r"(>=?\s*avg\(|<=?\s*avg\(|avg\(.*?\)\s*[<>]=?)"
TIME_COMPARE = r"\b(date|time|timestamp|interval)\b[^;]{0,60}\b(avg|mean)\s*\("


def classify_question(question: str, sql: str = None) -> str:
    if sql:
        return classify_using_sql_keywords(sql)
    return classify_using_keywords(question)


def classify_using_sql_keywords(query: str) -> str:
    s = query.lower()

    is_anomaly = (
        re.search(ANOMALY_TERMS, s)
        or re.search(AVG_COMPARE, s)
        or re.search(TIME_COMPARE, s)
    )

    is_order_dependent = (
        re.search(ORDER_STATEFUL_FUNCS, s)
        or re.search(RUNNING_TERMS, s)
    )

    if is_anomaly:
        return "Incident"  # anomaly takes priority on overlap

    if is_order_dependent:
        return "Stateful_No_Incident"

    return "Stateless"
