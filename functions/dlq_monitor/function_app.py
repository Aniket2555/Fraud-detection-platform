"""
DLQ Monitor Function: Polls Dead-Letter Queues across all subscriptions.
Logs details to Application Insights and raises alerts on non-empty DLQ.
Triggered on a 5-minute timer schedule.
"""

import json
import logging
import os
import azure.functions as func
from azure.servicebus import ServiceBusClient, ServiceBusSubQueue

app = func.FunctionApp()

TOPIC_NAME = "sb-topic-fraud-events"
SUBSCRIPTIONS = ["sub-stepup-auth", "sub-case-mgmt", "sub-audit-log"]


@app.timer_trigger(schedule="0 */5 * * * *", arg_name="timer", run_on_startup=False)
def dlq_monitor(timer: func.TimerRequest):
    """Monitors DLQ across all subscriptions every 5 minutes."""
    conn_str = os.environ.get("SERVICE_BUS_CONN_STR")
    if not conn_str:
        logging.warning("SERVICE_BUS_CONN_STR not set. DLQ monitor skipped.")
        return

    total_dlq_messages = 0

    with ServiceBusClient.from_connection_string(conn_str) as client:
        for sub_name in SUBSCRIPTIONS:
            try:
                with client.get_subscription_receiver(
                    topic_name=TOPIC_NAME,
                    subscription_name=sub_name,
                    sub_queue=ServiceBusSubQueue.DEAD_LETTER,
                    max_wait_time=5
                ) as receiver:
                    messages = receiver.receive_messages(max_message_count=50, max_wait_time=3)
                    count = len(messages)
                    total_dlq_messages += count

                    for msg in messages:
                        logging.error(
                            f"DLQ_ALERT | Subscription={sub_name} | "
                            f"Reason={msg.dead_letter_reason} | "
                            f"Error={msg.dead_letter_error_description} | "
                            f"MessageId={msg.message_id}"
                        )

            except Exception as e:
                logging.error(f"DLQ monitor error for {sub_name}: {e}")

    if total_dlq_messages > 0:
        logging.critical(f"🚨 DLQ ALERT: {total_dlq_messages} dead-lettered messages found across subscriptions!")
    else:
        logging.info("✅ DLQ Monitor: All subscription DLQs are empty.")
