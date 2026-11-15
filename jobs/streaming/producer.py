"""Streaming - step 2: publish live Wikipedia change events to Kafka.

Reads the Wikimedia EventStreams ``recentchange`` feed (Server-Sent
Events) and publishes every article edit / page creation of the selected
wikis to a Kafka topic. Filtering at the producer is kept broad (wiki,
namespace 0 = articles, edit/new): the IMDb-specific filtering happens in
Spark so that it can be changed without restarting the producer.

``--since-minutes N`` replays the last N minutes of the feed first: those
events arrive with an old event time, which exercises the late-data
handling (watermark) of the Spark job.

Usage (inside the Spark container, Kafka reachable as kafka:19092):
    pip install confluent-kafka requests
    python jobs/streaming/producer.py --minutes 20
"""

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
KEEP_FIELDS = [
    "id", "type", "namespace", "title", "title_url", "comment", "timestamp",
    "user", "bot", "minor", "wiki", "server_name", "length", "revision",
]


def ensure_topic(bootstrap, topic, partitions):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    if topic in admin.list_topics(timeout=10).topics:
        return
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)]
    )
    futures[topic].result()
    print(f"created topic {topic}")


def sse_events(url, params, headers):
    """Yield parsed JSON events from a Server-Sent Events HTTP stream."""
    with requests.get(url, params=params, headers=headers, stream=True,
                      timeout=(10, 60)) as response:
        response.raise_for_status()
        data = []
        for raw in response.iter_lines(decode_unicode=True):
            if raw is None:
                continue
            if raw == "":
                if data:
                    try:
                        yield json.loads("\n".join(data))
                    except json.JSONDecodeError:
                        pass
                    data = []
            elif raw.startswith("data:"):
                data.append(raw[5:].strip())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minutes", type=float, default=20)
    parser.add_argument("--since-minutes", type=float, default=0)
    parser.add_argument("--wikis", default=os.environ.get(
        "WIKIS", "enwiki,frwiki"))
    parser.add_argument("--topic", default=os.environ.get(
        "KAFKA_TOPIC", "wiki-changes"))
    parser.add_argument("--partitions", type=int, default=3)
    args = parser.parse_args()

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    contact = os.environ.get("CONTACT_EMAIL", "unknown")
    wikis = set(args.wikis.split(","))
    ensure_topic(bootstrap, args.topic, args.partitions)
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "client.id": "wiki-producer",
        "compression.type": "lz4",
        "linger.ms": 100,
    })

    headers = {"User-Agent": "ECE-BigData-IMDb-Stream/1.0 "
                             f"(student project; contact: {contact})"}
    params = {}
    if args.since_minutes:
        since = datetime.now(timezone.utc) - timedelta(
            minutes=args.since_minutes)
        params["since"] = since.strftime("%Y-%m-%dT%H:%M:%SZ")

    stop_at = time.monotonic() + args.minutes * 60
    seen = sent = 0
    last_id = None
    while time.monotonic() < stop_at:
        try:
            for event in sse_events(STREAM_URL, params, headers):
                seen += 1
                meta = event.get("meta", {})
                if (event.get("wiki") in wikis
                        and event.get("namespace") == 0
                        and event.get("type") in ("edit", "new")):
                    record = {k: event.get(k) for k in KEEP_FIELDS}
                    record["event_id"] = meta.get("id")
                    record["event_dt"] = meta.get("dt")
                    producer.produce(
                        args.topic, key=event.get("title", "").encode(),
                        value=json.dumps(record).encode("utf-8"),
                    )
                    sent += 1
                    producer.poll(0)
                last_id = meta.get("dt", last_id)
                if seen % 1000 == 0:
                    print(f"{seen} events read, {sent} published")
                if time.monotonic() >= stop_at:
                    break
        except (requests.RequestException, ConnectionError) as err:
            print(f"stream interrupted ({err}); reconnecting in 5s")
            time.sleep(5)
            if last_id:
                params["since"] = last_id
    producer.flush(30)
    print(f"finished: {seen} events read, {sent} published to {args.topic}")


if __name__ == "__main__":
    main()
