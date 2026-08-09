"""
DLQ Replay Handler: Inspects and re-publishes dead-lettered Service Bus messages.
"""

import os
import json
import logging
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSubQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dlq_replay")

TOPIC_NAME = "sb-topic-fraud-events"


def replay_dlq_messages(subscription_name: str, max_messages: int = 50):
    conn_str = os.environ.get("SERVICE_BUS_CONN_STR")
    if not conn_str:
        logger.error("SERVICE_BUS_CONN_STR environment variable not set.")
        return

    with ServiceBusClient.from_connection_string(conn_str) as client:
        sender = client.get_topic_sender(topic_name=TOPIC_NAME)
        receiver = client.get_subscription_receiver(
            topic_name=TOPIC_NAME,
            subscription_name=subscription_name,
            sub_queue=ServiceBusSubQueue.DEAD_LETTER
        )

        with receiver, sender:
            messages = receiver.receive_messages(max_message_count=max_messages, max_wait_time=5)
            logger.info(f"Retrieved {len(messages)} messages from DLQ of {subscription_name}")

            for msg in messages:
                body_str = str(msg)
                logger.info(f"Replaying message ID {msg.message_id}: {body_str}")

                new_msg = ServiceBusMessage(
                    body_str,
                    application_properties=msg.application_properties,
                    message_id=f"replay_{msg.message_id}"
                )
                sender.send_messages(new_msg)
                receiver.complete_message(msg)

            logger.info("DLQ replay complete.")


if __name__ == "__main__":
    import sys
    sub = sys.argv[1] if len(sys.argv) > 1 else "sub-case-mgmt"
    replay_dlq_messages(sub)
