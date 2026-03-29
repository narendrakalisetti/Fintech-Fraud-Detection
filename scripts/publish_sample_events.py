"""
Publish sample transaction events to Azure Event Hubs for smoke testing.
Usage: python scripts/publish_sample_events.py --file sample_data/sample_transactions.csv
"""
import argparse
import csv
import json
import os
from azure.eventhub import EventHubProducerClient, EventData

def publish_events(csv_file: str, connection_str: str, topic: str = "transactions"):
    producer = EventHubProducerClient.from_connection_string(
        connection_str, eventhub_name=topic
    )
    with producer:
        with open(csv_file, newline="") as f:
            reader = csv.DictReader(f)
            batch = producer.create_batch()
            count = 0
            for row in reader:
                if row.get("Transaction_Amount_GBP"):
                    row["Transaction_Amount_GBP"] = float(row["Transaction_Amount_GBP"])
                batch.add(EventData(json.dumps(row)))
                count += 1
            producer.send_batch(batch)
    print(f"Published {count} events to Event Hubs topic: {topic}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True)
    parser.add_argument("--topic", default="transactions")
    args = parser.parse_args()
    conn_str = os.environ.get("EH_CONNECTION_STRING", "")
    if not conn_str:
        raise ValueError("Set EH_CONNECTION_STRING environment variable")
    publish_events(args.file, conn_str, args.topic)
