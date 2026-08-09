# Transaction Stream Producer

Replays the Kaggle Credit Card Fraud Detection dataset into Azure Event Hubs as real-time `TransactionEvent` stream objects.

## Usage
1. Set connection string:
   ```bash
   export EVENTHUB_PRODUCER_CONN_STR="<your-eventhubs-producer-conn-string>"
   ```

2. Run stream producer:
   ```bash
   SPEED_MULTIPLIER=100 MAX_EVENTS=10000 python producer.py
   ```
