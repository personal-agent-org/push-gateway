use prometheus::{
    CounterVec, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};

const OUTCOMES: [&str; 3] = ["delivered", "unregistered", "upstream_error"];
const PRIORITIES: [&str; 2] = ["high", "normal"];
const REASONS: [&str; 9] = [
    "ip_rate_limit",
    "burst_rate_limit",
    "daily_quota",
    "global_rate_limit",
    "tombstoned",
    "invalid",
    "too_large",
    "not_configured",
    "store_unavailable",
];
const OPS: [&str; 4] = ["incr", "get", "set", "has"];

pub struct Metrics {
    registry: Registry,
    sends: CounterVec,
    rejected: CounterVec,
    relay: HistogramVec,
    store: CounterVec,
}
impl Metrics {
    pub fn new(version: &str, configured: bool, backend: &str) -> Self {
        let registry = Registry::new();
        let sends = CounterVec::new(
            Opts::new(
                "push_gateway_sends_total",
                "Relay attempts that reached Firebase, by outcome.",
            ),
            &["outcome", "priority"],
        )
        .unwrap();
        let rejected = CounterVec::new(
            Opts::new(
                "push_gateway_rejected_total",
                "Requests refused before reaching Firebase, by reason.",
            ),
            &["reason"],
        )
        .unwrap();
        let relay = HistogramVec::new(
            HistogramOpts::new(
                "push_gateway_relay_duration_seconds",
                "Time spent in the Firebase call, by outcome.",
            )
            .buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0]),
            &["outcome"],
        )
        .unwrap();
        let store = CounterVec::new(Opts::new("push_gateway_store_failures_total", "Rate-limit store operations that failed, by operation. A decision read that fails refuses the push; a best-effort write is ignored."), &["op"]).unwrap();
        let build = GaugeVec::new(
            Opts::new(
                "push_gateway_build_info",
                "Always 1; the version rides in the label.",
            ),
            &["version"],
        )
        .unwrap();
        let upstream = Gauge::new(
            "push_gateway_upstream_configured",
            "1 when FCM credentials are loaded; 0 means every push is refused.",
        )
        .unwrap();
        let store_backend = GaugeVec::new(Opts::new("push_gateway_store_backend", "Always 1; the backend rides in the label. 'memory' does not survive a restart and is not shared between replicas, so limits are per-process."), &["backend"]).unwrap();
        registry.register(Box::new(sends.clone())).unwrap();
        registry.register(Box::new(rejected.clone())).unwrap();
        registry.register(Box::new(relay.clone())).unwrap();
        registry.register(Box::new(store.clone())).unwrap();
        registry.register(Box::new(build.clone())).unwrap();
        registry.register(Box::new(upstream.clone())).unwrap();
        registry.register(Box::new(store_backend.clone())).unwrap();
        #[cfg(target_os = "linux")]
        registry
            .register(Box::new(
                prometheus::process_collector::ProcessCollector::for_self(),
            ))
            .unwrap();
        for o in OUTCOMES {
            for p in PRIORITIES {
                sends.with_label_values(&[o, p]);
            }
            relay.with_label_values(&[o]);
        }
        for r in REASONS {
            rejected.with_label_values(&[r]);
        }
        for op in OPS {
            store.with_label_values(&[op]);
        }
        build.with_label_values(&[version]).set(1.);
        upstream.set(if configured { 1. } else { 0. });
        store_backend.with_label_values(&[backend]).set(1.);
        Self {
            registry,
            sends,
            rejected,
            relay,
            store,
        }
    }
    pub fn sent(&self, outcome: &str, priority: &str, secs: f64) {
        self.sends.with_label_values(&[outcome, priority]).inc();
        self.relay.with_label_values(&[outcome]).observe(secs);
    }
    pub fn reject(&self, reason: &str) {
        self.rejected.with_label_values(&[reason]).inc();
    }
    pub fn store_failed(&self, op: &str) {
        self.store.with_label_values(&[op]).inc();
    }
    pub fn render(&self) -> Vec<u8> {
        let mut out = vec![];
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut out)
            .unwrap();
        out
    }
}
