"""
scripts/publish_sample_events.py
Smoke test: reads sample_transactions.csv and publishes to Azure Event Hubs.
Used to verify the streaming pipeline end-to-end after deployment.

Usage:
    python scripts/publish_sample_events.py --file sample_data/sample_transactions.csv
    python scripts/publish_sample_events.py --file sample_data/sample_transactions.csv --rows 500
"""

import argparse
import csv
import json
import os
import time

# azure-eventhub required: pip install azure-eventhub
try:
    from azure.eventhub import EventHubProducerClient, EventData
    from azure.identity import DefaultAzureCredential
except ImportError:
    print("Install: pip install azure-eventhub azure-identity")
    raise


def publish_events(
    csv_path: str,
    eventhub_namespace: str,
    eventhub_name: str,
    max_rows: int = None,
    batch_size: int = 100,
) -> None:
    """Publish rows from CSV as JSON events to Event Hubs."""

    credential = DefaultAzureCredential()
    fully_qualified_namespace = f"{eventhub_namespace}.servicebus.windows.net"

    producer = EventHubProducerClient(
        fully_qualified_namespace=fully_qualified_namespace,
        eventhub_name=eventhub_name,
        credential=credential,
    )

    total_sent = 0

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        if max_rows:
            rows = rows[:max_rows]

    print(f"Publishing {len(rows)} events to {eventhub_name}...")

    with producer:
        batch = producer.create_batch()
        for i, row in enumerate(rows):
            event_body = json.dumps(row)
            try:
                batch.add(EventData(event_body))
            except ValueError:
                # Batch full — send current batch and start new one
                producer.send_batch(batch)
                total_sent += len(batch)
                batch = producer.create_batch()
                batch.add(EventData(event_body))

            if (i + 1) % batch_size == 0:
                producer.send_batch(batch)
                total_sent += len(batch)
                batch = producer.create_batch()
                print(f"  Sent {total_sent} / {len(rows)} events...")
                time.sleep(0.1)   # Avoid throttling on Standard tier

        # Send remaining
        if len(batch) > 0:
            producer.send_batch(batch)
            total_sent += len(batch)

    print(f"Done. Total events sent: {total_sent}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish sample events to Event Hubs")
    parser.add_argument("--file",      required=True,  help="Path to CSV file")
    parser.add_argument("--namespace", default=os.getenv("EVENTHUB_NAMESPACE", "eh-uks-clearpay-prod"),
                        help="Event Hubs namespace name")
    parser.add_argument("--hub",       default=os.getenv("EVENTHUB_NAME", "transactions"),
                        help="Event Hub name")
    parser.add_argument("--rows",      type=int, default=None, help="Max rows to send (default: all)")
    args = parser.parse_args()

    publish_events(
        csv_path=args.file,
        eventhub_namespace=args.namespace,
        eventhub_name=args.hub,
        max_rows=args.rows,
    )
