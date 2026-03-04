import csv
from datetime import datetime, timezone
import re
import requests

SAMPLE_DATA_URL_MAP = {
    "weather": "https://docs.influxdata.com/downloads/bay-area-weather.lp",
    "bitcoin": "https://docs.influxdata.com/downloads/bitcoin.lp",
    "wind_data": "https://docs.influxdata.com/downloads/eu-wind-data.lp",
}


def fmt_timestamp(raw_ts: str):
    len_ts = len(raw_ts)

    raw_ts_int = int(raw_ts)
    ts_s = raw_ts_int
    if len_ts == 19:
        ts_s = raw_ts_int // 1e9
    elif len_ts == 16:
        ts_s = raw_ts_int // 1e6
    elif len_ts == 13:
        ts_s = raw_ts_int // 1000

    datetime_obj = datetime.fromtimestamp(ts_s, tz=timezone.utc)
    return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")


def lp_to_csv(lp_data: str, out_fp: str):
    lines = lp_data.splitlines()

    data_dict_list = []
    for line in lines:
        if not line:
            continue

        # We use a regex here to avoid errors where the values contain spaces
        parts = re.split(r"(?<!\\) ", line, maxsplit=2)

        tags = parts[0].split(",")
        fields = parts[1].split(",")
        timestamp = parts[2]

        data_dict = {}
        for t in tags[1:]:
            k, v = t.split("=")
            v = v.replace("\\ ", " ")
            data_dict[k] = v

        for f in fields:
            k, v = f.split("=")
            v = v.replace("\\ ", " ")
            if v.endswith("i"):
                v = v[:-1]
            data_dict[k] = v

        data_dict["timestamp"] = fmt_timestamp(timestamp)

        data_dict_list.append(data_dict)

    with open(out_fp, "w") as f:
        writer = csv.DictWriter(f, fieldnames=data_dict_list[0].keys())
        writer.writeheader()
        writer.writerows(data_dict_list)
        print(f"Saved data to {out_fp}!")


if __name__ == "__main__":
    for name, url in SAMPLE_DATA_URL_MAP.items():
        print(f"Getting data for {name}...")
        response = requests.get(url)
        lp_data = response.content.decode("utf-8")
        lp_to_csv(lp_data=lp_data, out_fp=f"{name}.csv")
