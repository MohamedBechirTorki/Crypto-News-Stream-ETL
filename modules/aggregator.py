import json
from datetime import datetime, timezone, timedelta
from dateutil import parser as dateutil_parser


INPUT_PATH = "data/cleaned/data.json"


def load_data():
    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def filter_time_window(articles, hours=1):
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=hours)

    out = []
    for a in articles:
        pub = dateutil_parser.parse(a["published_at"])
        if pub.tzinfo is None:
            pub = pub.replace(tzinfo=timezone.utc)

        if start <= pub <= now:
            out.append(a)

    return out


def simple_signal_score(a):
    # your cleaner already does base filtering
    return a.get("quality_score", 0)


def aggregate():
    data = load_data()

    last_hour = filter_time_window(data, 1)
    last_6h = filter_time_window(data, 6)

    print("1h:", len(last_hour))
    print("6h:", len(last_6h))

    # placeholder for next stage (LLM)
    for a in last_hour:
        a["signal_score"] = simple_signal_score(a)

    return last_hour


if __name__ == "__main__":
    aggregate()