"""Prometheus instruments for the relay.

What was here before was one hand-rolled counter, ``sends_total{outcome}``, rendered by
string concatenation. It answered "how many pushes went out" and nothing else -- in
particular it could not answer the two questions an operator actually has when something is
wrong:

* **Why was a request refused?** Four different causes all counted as ``rate_limited``: a
  noisy instance (per-IP), a looping device (per-token burst), a device that spent its daily
  budget, and the gateway itself being saturated (global). Those mean completely different
  things and one of them is not even the caller's fault.
* **Is the limiter still working?** ``RateLimiter`` fails OPEN on store errors and logs a
  warning. If Redis goes away every limit silently stops applying, and the old metrics showed
  a perfectly healthy gateway while it happened.

Instruments live on a registry PER APP rather than the process-global default, so building
two apps in one test process does not make them share counters -- which is how the existing
tests are written.

Single uvicorn process (see the Dockerfile). Adding ``--workers`` later would make each
worker export only its own numbers; that needs ``prometheus_client``'s multiprocess mode, not
just a bigger number in the CMD.
"""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.gc_collector import GCCollector
from prometheus_client.platform_collector import PlatformCollector
from prometheus_client.process_collector import ProcessCollector

#: Outcomes of a relay attempt that REACHED Firebase.
SEND_OUTCOMES = ("delivered", "unregistered", "upstream_error")

#: Why a request never reached Firebase. A closed set, deliberately: the reason has to stay a
#: label with bounded cardinality.
REJECT_REASONS = (
    "ip_rate_limit",  # the calling instance is sending too fast
    "burst_rate_limit",  # one device token is looping
    "daily_quota",  # that device spent its budget for the day
    "global_rate_limit",  # the gateway itself is saturated
    "tombstoned",  # the token was already known to be dead
    "invalid",  # malformed frame
    "too_large",  # body over the cap
    "not_configured",  # no FCM credentials on this gateway
    "store_unavailable",  # the limiter could not decide, so nothing was relayed
)

#: Request priorities. Bounded by the schema (Literal["high", "normal"]).
PRIORITIES = ("high", "normal")

#: Store operations that can fail. Bounded by the wrapper methods in limits.py.
STORE_OPS = ("incr", "get", "set", "has")

# FCM over the public internet: sub-second normally, seconds when Google is unhappy. The top
# bucket sits above the client timeout so a run of them is visibly "everything timed out"
# rather than blending into +Inf.
_RELAY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0)


class Metrics:
    """One app's instruments, plus the exposition rendering."""

    def __init__(self, *, version: str, upstream_configured: bool, store_backend: str) -> None:
        self.registry = CollectorRegistry()
        # Process/platform/GC series are worth having and are free -- but a custom registry
        # gets none of them by default, so they are registered explicitly.
        ProcessCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        GCCollector(registry=self.registry)

        self.sends = Counter(
            "push_gateway_sends_total",
            "Relay attempts that reached Firebase, by outcome.",
            ("outcome", "priority"),
            registry=self.registry,
        )
        self.rejected = Counter(
            "push_gateway_rejected_total",
            "Requests refused before reaching Firebase, by reason.",
            ("reason",),
            registry=self.registry,
        )
        self.relay_duration = Histogram(
            "push_gateway_relay_duration_seconds",
            "Time spent in the Firebase call, by outcome.",
            ("outcome",),
            buckets=_RELAY_BUCKETS,
            registry=self.registry,
        )
        self.store_failures = Counter(
            "push_gateway_store_failures_total",
            "Rate-limit store operations that failed, by operation. A decision read that "
            "fails refuses the push; a best-effort write is ignored.",
            ("op",),
            registry=self.registry,
        )
        self.build = Gauge(
            "push_gateway_build_info",
            "Always 1; the version rides in the label.",
            ("version",),
            registry=self.registry,
        )
        self.upstream_configured = Gauge(
            "push_gateway_upstream_configured",
            "1 when FCM credentials are loaded; 0 means every push is refused.",
            registry=self.registry,
        )
        self.store_backend = Gauge(
            "push_gateway_store_backend",
            "Always 1; the backend rides in the label. 'memory' does not survive a restart "
            "and is not shared between replicas, so limits are per-process.",
            ("backend",),
            registry=self.registry,
        )

        # Pre-create every series so a quiet gateway exports zeros instead of nothing. A
        # counter that is absent until its first event cannot be alerted on with rate(), and
        # "no data" reads the same as "no problem".
        for outcome in SEND_OUTCOMES:
            for priority in PRIORITIES:
                self.sends.labels(outcome=outcome, priority=priority)
            self.relay_duration.labels(outcome=outcome)
        for reason in REJECT_REASONS:
            self.rejected.labels(reason=reason)
        for op in STORE_OPS:
            self.store_failures.labels(op=op)

        self.build.labels(version=version).set(1)
        self.upstream_configured.set(1 if upstream_configured else 0)
        self.store_backend.labels(backend=store_backend).set(1)

    def sent(self, outcome: str, priority: str, seconds: float) -> None:
        self.sends.labels(outcome=outcome, priority=priority).inc()
        self.relay_duration.labels(outcome=outcome).observe(seconds)

    def reject(self, reason: str) -> None:
        self.rejected.labels(reason=reason).inc()

    def store_failed(self, op: str) -> None:
        self.store_failures.labels(op=op).inc()

    def render(self) -> bytes:
        return generate_latest(self.registry)
