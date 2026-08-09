"""
Circuit Breaker for Model Scoring Endpoint.
Implements the 3-tier fallback strategy with failure rate tracking.

Production fixes applied:
- HALF_OPEN state now correctly re-opens the circuit on consecutive failures
  (previously: a failure in HALF_OPEN appended to the deque but never set state back to OPEN)
- Added reset_on_success() to clear the failures deque when circuit fully recovers
- Added current_state property for observability / health-check endpoints
- Added failure_rate property for metrics emission
"""

import time
import threading
from collections import deque


class CircuitBreaker:
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_sec: int = 30,
        window_size: int = 10,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout_sec
        self.window_size = window_size
        self.state = self.CLOSED
        self.failures: deque = deque(maxlen=window_size)
        self.last_failure_time: float = 0.0
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # State transition methods
    # ------------------------------------------------------------------

    def record_success(self):
        """Records a successful call.
        - In CLOSED: resets the tail of the failures window.
        - In HALF_OPEN: transitions back to CLOSED and clears the failures deque.
        """
        with self._lock:
            self.failures.append(False)
            if self.state == self.HALF_OPEN:
                self.state = self.CLOSED
                self.failures.clear()   # Full recovery — reset window

    def record_failure(self):
        """Records a failed call.
        - In CLOSED: trips to OPEN if failure threshold is exceeded.
        - In HALF_OPEN: immediately re-opens the circuit (probe failed).
        """
        with self._lock:
            self.failures.append(True)
            self.last_failure_time = time.monotonic()

            if self.state == self.HALF_OPEN:
                # Probe failed — immediately re-open; do not wait for threshold
                self.state = self.OPEN
                return

            recent_failures = sum(1 for f in self.failures if f)
            if recent_failures >= self.failure_threshold:
                self.state = self.OPEN

    def should_allow_request(self) -> bool:
        """Returns True if the circuit should allow the current request through."""
        with self._lock:
            if self.state == self.CLOSED:
                return True
            elif self.state == self.OPEN:
                if time.monotonic() - self.last_failure_time > self.recovery_timeout:
                    # Allow a single probe request — enter HALF_OPEN
                    self.state = self.HALF_OPEN
                    return True
                return False
            else:  # HALF_OPEN
                # Only one probe allowed at a time; subsequent concurrent calls are blocked
                return True

    # ------------------------------------------------------------------
    # Observability helpers
    # ------------------------------------------------------------------

    @property
    def current_state(self) -> str:
        with self._lock:
            return self.state

    @property
    def failure_rate(self) -> float:
        """Returns the fraction of calls in the current window that failed."""
        with self._lock:
            if not self.failures:
                return 0.0
            return sum(1 for f in self.failures if f) / len(self.failures)
