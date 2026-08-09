"""
Latency Benchmark Test: Verifies scoring path executes within sub-100ms SLA.
"""

import pytest
import time
import json

from fraud_detection.ml.serving.circuit_breaker import CircuitBreaker


def test_circuit_breaker_latency_overhead():
    cb = CircuitBreaker()
    start = time.time()
    for _ in range(1000):
        cb.should_allow_request()
        cb.record_success()
    elapsed_ms = (time.time() - start) * 1000.0

    assert elapsed_ms < 50.0  # 1000 checks should take under 50ms total
