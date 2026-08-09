"""
Main producer: replays Kaggle Credit Card dataset into Azure Event Hubs.
"""
import asyncio
import csv
import json
import time
import logging
from datetime import datetime, timezone

import jsonschema
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

from config import ProducerConfig
from event_mapper import map_kaggle_row_to_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


class TransactionProducer:
    """Replays Kaggle Credit Card dataset as streaming events into Event Hubs."""

    def __init__(self, config: ProducerConfig):
        self.config = config
        self.client = EventHubProducerClient.from_connection_string(
            config.eventhub_conn_str,
            eventhub_name=config.eventhub_name,
        )
        self.time_anchor = datetime.fromisoformat(
            config.time_anchor.replace("Z", "+00:00")
        )
        self.events_sent = 0
        self.fraud_sent = 0
        self.errors = 0
        self.start_time = None

        self.schema = None
        if self.config.validate_schema:
            with open(self.config.schema_path, 'r') as f:
                self.schema = json.load(f)

    async def replay_dataset(self):
        """Replay the entire dataset chronologically into Event Hubs."""
        logger.info(
            f"Starting replay: speed={self.config.speed_multiplier}x, "
            f"batch_size={self.config.batch_size}, "
            f"max_events={self.config.max_events or 'all'}"
        )

        self.start_time = time.time()

        rows = []
        try:
            with open(self.config.dataset_path, 'r') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if self.config.max_events and i >= self.config.max_events:
                        break
                    rows.append(row)
        except FileNotFoundError:
            logger.warning(f"Dataset file {self.config.dataset_path} not found. Generating sample events.")
            rows = [{"Time": i * 10, "Amount": 50.0 + i, "Class": 1 if i % 20 == 0 else 0} for i in range(100)]

        rows.sort(key=lambda r: float(r.get("Time", 0)))
        logger.info(f"Loaded {len(rows)} rows, sorted chronologically")

        batch = []
        prev_time = 0.0
        last_logged_at = 0

        async with self.client:
            for idx, row in enumerate(rows):
                event = map_kaggle_row_to_event(row, idx, self.time_anchor)

                if self.schema is not None:
                    try:
                        jsonschema.validate(event, self.schema)
                    except jsonschema.ValidationError as e:
                        self.errors += 1
                        logger.error(f"Schema validation failed for row {idx}: {e.message}")
                        continue

                event_data = EventData(json.dumps(event))
                batch.append((event["card_id"], event_data))

                if event.get("is_fraud"):
                    self.fraud_sent += 1

                if len(batch) >= self.config.batch_size:
                    await self._send_batch(batch)
                    batch = []

                    current_time = float(row.get("Time", 0))
                    delay = (current_time - prev_time) / self.config.speed_multiplier
                    prev_time = current_time

                    if 0 < delay < 10:
                        await asyncio.sleep(delay)

                if self.events_sent - last_logged_at >= self.config.log_interval:
                    last_logged_at = self.events_sent
                    elapsed = time.time() - self.start_time
                    rate = self.events_sent / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"Progress: {self.events_sent}/{len(rows)} events sent "
                        f"({rate:.1f} events/s), fraud={self.fraud_sent}, errors={self.errors}"
                    )

            if batch:
                await self._send_batch(batch)

        elapsed = time.time() - self.start_time
        logger.info(
            f"Replay complete: {self.events_sent} events sent in {elapsed:.1f}s "
            f"({self.events_sent/max(elapsed, 0.001):.1f} events/s), "
            f"fraud={self.fraud_sent}, errors={self.errors}"
        )

    async def _send_batch(self, events: list):
        """Send a batch of (partition_key, EventData) pairs to Event Hubs.

        Event Hubs assigns a partition per EventDataBatch (via
        create_batch(partition_key=...)), not per event, so events are
        grouped by partition_key here to preserve per-card_id partition
        affinity that downstream velocity features depend on.
        """
        groups: dict = {}
        for partition_key, event_data in events:
            groups.setdefault(partition_key, []).append(event_data)

        for partition_key, group_events in groups.items():
            try:
                event_batch = await self.client.create_batch(partition_key=partition_key)
                for event_data in group_events:
                    try:
                        event_batch.add(event_data)
                    except ValueError:
                        await self.client.send_batch(event_batch)
                        event_batch = await self.client.create_batch(partition_key=partition_key)
                        event_batch.add(event_data)

                await self.client.send_batch(event_batch)
                self.events_sent += len(group_events)
            except Exception as e:
                self.errors += len(group_events)
                logger.error(f"Failed to send batch of {len(group_events)} events for partition_key={partition_key}: {e}")


async def main():
    config = ProducerConfig()
    producer = TransactionProducer(config)
    await producer.replay_dataset()


if __name__ == "__main__":
    asyncio.run(main())
