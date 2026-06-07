import argparse
import json
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="data/ad_events_sample.csv")
    parser.add_argument("--topic", default="ad-events")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--sleep", type=float, default=0.01)
    parser.add_argument("--max-events", type=int, default=1000)
    args = parser.parse_args()

    df = pd.read_csv(args.csv)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    print(f"[producer] csv={args.csv}")
    print(f"[producer] topic={args.topic}")
    print(f"[producer] rows={len(df)}")
    print(f"[producer] max_events={args.max_events}")

    count = 0

    for _, row in df.iterrows():
        if count >= args.max_events:
            break

        event = {
            "event_id": str(row["event_id"]),
            "event_time": str(row["event_time"]),
            "campaign_id": str(row["campaign_id"]),
            "user_id": str(row["user_id"]),
            "event_type": str(row["event_type"]),
            "amount": float(row["amount"]),
            "ingest_time": datetime.utcnow().isoformat(),
        }

        producer.send(args.topic, value=event)
        print(f"[sent] {event}")

        count += 1
        time.sleep(args.sleep)

    producer.flush()
    producer.close()

    print(f"[producer] done. sent={count}")


if __name__ == "__main__":
    main()