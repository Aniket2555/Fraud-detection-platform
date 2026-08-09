"""
Real-Time Graph Feature Queries against Cosmos DB Gremlin API.
"""

from gremlin_python.driver import client as gremlin_client


def get_device_sharing_count(g_client, device_id: str, hours: int = 24) -> int:
    """Returns distinct customer count sharing this device in the last N hours."""
    query = f"g.V('{device_id}').in('USED_DEVICE').dedup().count()"
    result = g_client.submit(query).all().result()
    return result[0] if result else 0


def get_ip_sharing_count(g_client, ip_hash: str, hours: int = 1) -> int:
    """Returns distinct card count from this IP hash in the last N hours."""
    query = f"g.V('{ip_hash}').in('USED_IP').dedup().count()"
    result = g_client.submit(query).all().result()
    return result[0] if result else 0
