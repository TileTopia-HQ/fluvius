//! Prometheus-compatible metrics for pipeline observability.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Metrics collector for pipeline operators.
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    events_received: AtomicU64,
    events_emitted: AtomicU64,
    events_filtered: AtomicU64,
    events_late: AtomicU64,
    processing_time_us: AtomicU64,
    processing_count: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                events_received: AtomicU64::new(0),
                events_emitted: AtomicU64::new(0),
                events_filtered: AtomicU64::new(0),
                events_late: AtomicU64::new(0),
                processing_time_us: AtomicU64::new(0),
                processing_count: AtomicU64::new(0),
            }),
        }
    }

    pub fn inc_received(&self) {
        self.inner.events_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_emitted(&self) {
        self.inner.events_emitted.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_filtered(&self) {
        self.inner.events_filtered.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_late(&self) {
        self.inner.events_late.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_processing_time(&self, microseconds: u64) {
        self.inner
            .processing_time_us
            .fetch_add(microseconds, Ordering::Relaxed);
        self.inner.processing_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn events_received(&self) -> u64 {
        self.inner.events_received.load(Ordering::Relaxed)
    }

    pub fn events_emitted(&self) -> u64 {
        self.inner.events_emitted.load(Ordering::Relaxed)
    }

    pub fn events_filtered(&self) -> u64 {
        self.inner.events_filtered.load(Ordering::Relaxed)
    }

    pub fn events_late(&self) -> u64 {
        self.inner.events_late.load(Ordering::Relaxed)
    }

    pub fn avg_processing_time_us(&self) -> f64 {
        let count = self.inner.processing_count.load(Ordering::Relaxed);
        if count == 0 {
            return 0.0;
        }
        self.inner.processing_time_us.load(Ordering::Relaxed) as f64 / count as f64
    }

    /// Render metrics in Prometheus exposition format.
    pub fn to_prometheus(&self, prefix: &str) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "# HELP {prefix}_events_received_total Total events received\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_events_received_total counter\n"));
        out.push_str(&format!(
            "{prefix}_events_received_total {}\n",
            self.events_received()
        ));

        out.push_str(&format!(
            "# HELP {prefix}_events_emitted_total Total events emitted\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_events_emitted_total counter\n"));
        out.push_str(&format!(
            "{prefix}_events_emitted_total {}\n",
            self.events_emitted()
        ));

        out.push_str(&format!(
            "# HELP {prefix}_events_filtered_total Total events filtered out\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_events_filtered_total counter\n"));
        out.push_str(&format!(
            "{prefix}_events_filtered_total {}\n",
            self.events_filtered()
        ));

        out.push_str(&format!(
            "# HELP {prefix}_events_late_total Total late events\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_events_late_total counter\n"));
        out.push_str(&format!(
            "{prefix}_events_late_total {}\n",
            self.events_late()
        ));

        out.push_str(&format!(
            "# HELP {prefix}_processing_time_avg_us Average processing time in microseconds\n"
        ));
        out.push_str(&format!("# TYPE {prefix}_processing_time_avg_us gauge\n"));
        out.push_str(&format!(
            "{prefix}_processing_time_avg_us {:.2}\n",
            self.avg_processing_time_us()
        ));

        out
    }

    /// Reset all counters.
    pub fn reset(&self) {
        self.inner.events_received.store(0, Ordering::Relaxed);
        self.inner.events_emitted.store(0, Ordering::Relaxed);
        self.inner.events_filtered.store(0, Ordering::Relaxed);
        self.inner.events_late.store(0, Ordering::Relaxed);
        self.inner.processing_time_us.store(0, Ordering::Relaxed);
        self.inner.processing_count.store(0, Ordering::Relaxed);
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basic() {
        let m = Metrics::new();
        m.inc_received();
        m.inc_received();
        m.inc_emitted();
        m.inc_filtered();
        m.inc_late();
        m.record_processing_time(100);
        m.record_processing_time(200);

        assert_eq!(m.events_received(), 2);
        assert_eq!(m.events_emitted(), 1);
        assert_eq!(m.events_filtered(), 1);
        assert_eq!(m.events_late(), 1);
        assert_eq!(m.avg_processing_time_us(), 150.0);
    }

    #[test]
    fn test_prometheus_format() {
        let m = Metrics::new();
        m.inc_received();
        let output = m.to_prometheus("fluvius");
        assert!(output.contains("fluvius_events_received_total 1"));
        assert!(output.contains("# TYPE fluvius_events_received_total counter"));
    }
}
