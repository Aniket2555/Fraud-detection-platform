"""
Maps Kaggle Credit Card dataset rows to the canonical TransactionEvent schema.
All synthesis is deterministic (hash-based) for reproducibility.
"""
import hashlib
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Any

CITY_POOL = [
    {"city": "Mumbai", "lat": 19.076, "lon": 72.877, "country": "IN"},
    {"city": "Delhi", "lat": 28.613, "lon": 77.209, "country": "IN"},
    {"city": "Bangalore", "lat": 12.971, "lon": 77.594, "country": "IN"},
    {"city": "New York", "lat": 40.712, "lon": -74.006, "country": "US"},
    {"city": "London", "lat": 51.507, "lon": -0.127, "country": "GB"},
    {"city": "Singapore", "lat": 1.352, "lon": 103.819, "country": "SG"},
    {"city": "Dubai", "lat": 25.204, "lon": 55.270, "country": "AE"},
    {"city": "Tokyo", "lat": 35.676, "lon": 139.650, "country": "JP"},
    {"city": "Sydney", "lat": -33.868, "lon": 151.209, "country": "AU"},
    {"city": "São Paulo", "lat": -23.550, "lon": -46.633, "country": "BR"},
]

MERCHANT_CATEGORIES = [
    ("retail", 0.25), ("ecommerce", 0.30), ("grocery", 0.15),
    ("gas", 0.08), ("travel", 0.05), ("dining", 0.10),
    ("entertainment", 0.04), ("other", 0.03)
]

CHANNELS = [
    ("mobile_app", 0.40), ("web", 0.35), ("pos", 0.20), ("atm", 0.05)
]


def _deterministic_hash(seed: str, length: int = 10) -> str:
    """Generate a deterministic hash string from a seed."""
    return hashlib.sha256(seed.encode()).hexdigest()[:length]


def _weighted_choice(options: list, seed: int) -> str:
    """Deterministic weighted random selection."""
    rng = random.Random(seed)
    items, weights = zip(*options)
    return rng.choices(items, weights=weights, k=1)[0]


def map_kaggle_row_to_event(
    row: Dict[str, Any],
    row_index: int,
    time_anchor: datetime
) -> Dict[str, Any]:
    """Map a single Kaggle Credit Card dataset row to a canonical TransactionEvent."""
    v1 = str(row.get("V1", 0))
    v2 = str(row.get("V2", 0))
    v3 = str(row.get("V3", 0))
    v4 = str(row.get("V4", 0))
    v5 = str(row.get("V5", 0))
    v12 = str(row.get("V12", 0))
    v13 = str(row.get("V13", 0))
    v14 = str(row.get("V14", 0))

    customer_id = f"cust_{_deterministic_hash(f'{v1}_{v2}_{v3}', 8)}"
    card_id = f"card_{_deterministic_hash(f'{v1}_{v2}_{v4}_{v5}', 10)}"
    merchant_id = f"merch_{_deterministic_hash(f'{v12}_{v13}_{v14}', 8)}"

    time_offset = float(row.get("Time", 0))
    event_time = time_anchor + timedelta(seconds=time_offset)

    city_index = int(_deterministic_hash(customer_id, 4), 16) % len(CITY_POOL)
    base_city = CITY_POOL[city_index]
    geo_rng = random.Random(row_index)
    lat = base_city["lat"] + geo_rng.gauss(0, 0.05)
    lon = base_city["lon"] + geo_rng.gauss(0, 0.05)

    amount = float(row.get("Amount", 0))
    channel = _weighted_choice(CHANNELS, row_index)
    payment_method = "credit_card" if (row_index % 100) < 85 else "debit_card"
    merchant_category = _weighted_choice(MERCHANT_CATEGORIES, row_index + 7)

    day_of_event = event_time.day
    hour_of_event = event_time.hour
    device_id = f"dev_{_deterministic_hash(f'{customer_id}_{day_of_event % 3}', 8)}"
    session_id = f"sess_{_deterministic_hash(f'{customer_id}_{hour_of_event}', 8)}"
    ip_hash = _deterministic_hash(f"{device_id}_{hour_of_event}", 16)

    billing_country = base_city["country"]
    is_cross_border = (row_index % 20) == 0
    shipping_country = (
        CITY_POOL[(city_index + 1) % len(CITY_POOL)]["country"]
        if is_cross_border else billing_country
    )

    return {
        "transaction_id": str(uuid.uuid4()),
        "schema_version": "1.0",
        "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "ingestion_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "customer_id": customer_id,
        "card_id": card_id,
        "amount": round(amount, 2),
        "currency": "USD",
        "merchant_id": merchant_id,
        "merchant_category": merchant_category,
        "payment_method": payment_method,
        "device_id": device_id,
        "ip_address": ip_hash,
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "billing_country": billing_country,
        "shipping_country": shipping_country,
        "channel": channel,
        "is_recurring": (row_index % 12) == 0,
        "session_id": session_id,
        "is_fraud": bool(float(row.get("Class", 0)) == 1),
        "is_synthetic": False,
        "source": "kaggle_credit_card_replay",
    }
