//! Prometheus metrics for worker observability and autoscaling signals.

use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use std::sync::Arc;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct JobTypeLabel(pub String);

impl prometheus_client::encoding::EncodeLabelSet for JobTypeLabel {
    fn encode(
        &self,
        mut encoder: prometheus_client::encoding::LabelSetEncoder,
    ) -> Result<(), std::fmt::Error> {
        use prometheus_client::encoding::EncodeLabel;
        ("type", self.0.as_str()).encode(encoder.encode_label())?;
        Ok(())
    }
}

#[derive(Clone)]
pub struct WorkerMetrics {
    pub stage_queue_depth: Gauge,
    pub exec_queue_depth: Gauge,
    pub active_jobs: Gauge,
    pub jobs_completed: Family<JobTypeLabel, Counter>,
    pub reaper_reclaimed: Counter,
    pub reaper_failed: Counter,
    pub rows_processed: Counter,
    pub registry: Arc<Registry>,
}

impl WorkerMetrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let stage_queue_depth = Gauge::default();
        registry.register(
            "kalla_stage_queue_depth",
            "Number of pending stage jobs",
            stage_queue_depth.clone(),
        );

        let exec_queue_depth = Gauge::default();
        registry.register(
            "kalla_exec_queue_depth",
            "Number of pending exec jobs",
            exec_queue_depth.clone(),
        );

        let active_jobs = Gauge::default();
        registry.register(
            "kalla_worker_active_jobs",
            "Number of jobs currently being processed",
            active_jobs.clone(),
        );

        let jobs_completed = Family::<JobTypeLabel, Counter>::default();
        registry.register(
            "kalla_worker_jobs_completed_total",
            "Total jobs completed by type",
            jobs_completed.clone(),
        );

        let reaper_reclaimed = Counter::default();
        registry.register(
            "kalla_reaper_jobs_reclaimed_total",
            "Jobs reclaimed by reaper",
            reaper_reclaimed.clone(),
        );

        let reaper_failed = Counter::default();
        registry.register(
            "kalla_reaper_jobs_failed_total",
            "Jobs permanently failed by reaper",
            reaper_failed.clone(),
        );

        let rows_processed = Counter::default();
        registry.register(
            "kalla_worker_rows_processed_total",
            "Total rows processed across all jobs",
            rows_processed.clone(),
        );

        Self {
            stage_queue_depth,
            exec_queue_depth,
            active_jobs,
            jobs_completed,
            reaper_reclaimed,
            reaper_failed,
            rows_processed,
            registry: Arc::new(registry),
        }
    }

    /// Encode all metrics as Prometheus text format.
    pub fn encode(&self) -> String {
        let mut buf = String::new();
        encode(&mut buf, &self.registry).unwrap();
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_all_metrics() {
        let metrics = WorkerMetrics::new();
        let output = metrics.encode();

        // Verify all registered metric names appear in output
        assert!(output.contains("kalla_stage_queue_depth"));
        assert!(output.contains("kalla_exec_queue_depth"));
        assert!(output.contains("kalla_worker_active_jobs"));
        assert!(output.contains("kalla_worker_jobs_completed_total"));
        assert!(output.contains("kalla_reaper_jobs_reclaimed_total"));
        assert!(output.contains("kalla_reaper_jobs_failed_total"));
        assert!(output.contains("kalla_worker_rows_processed_total"));
    }

    #[test]
    fn encode_produces_valid_prometheus_text() {
        let metrics = WorkerMetrics::new();
        let output = metrics.encode();

        // Prometheus text format has HELP and TYPE lines
        assert!(output.contains("# HELP"));
        assert!(output.contains("# TYPE"));
        // Gauges should be typed as gauge
        assert!(output.contains("# TYPE kalla_stage_queue_depth gauge"));
        // Counters should be typed as counter
        assert!(output.contains("# TYPE kalla_reaper_jobs_reclaimed_total counter"));
    }

    #[test]
    fn gauge_set_reflected_in_encode() {
        let metrics = WorkerMetrics::new();
        metrics.stage_queue_depth.set(42);
        metrics.exec_queue_depth.set(7);

        let output = metrics.encode();
        assert!(
            output.contains("kalla_stage_queue_depth 42"),
            "Expected gauge value 42 in output: {output}"
        );
        assert!(
            output.contains("kalla_exec_queue_depth 7"),
            "Expected gauge value 7 in output: {output}"
        );
    }

    #[test]
    fn counter_inc_reflected_in_encode() {
        let metrics = WorkerMetrics::new();
        metrics.reaper_reclaimed.inc();
        metrics.reaper_reclaimed.inc();
        metrics.rows_processed.inc_by(100);

        let output = metrics.encode();
        // prometheus-client appends _total to counter names per OpenMetrics spec,
        // so registered name "kalla_reaper_jobs_reclaimed_total" encodes as
        // "kalla_reaper_jobs_reclaimed_total_total".
        assert!(
            output.contains("kalla_reaper_jobs_reclaimed_total_total 2"),
            "Expected counter value 2 in output: {output}"
        );
        assert!(
            output.contains("kalla_worker_rows_processed_total_total 100"),
            "Expected counter value 100 in output: {output}"
        );
    }

    #[test]
    fn family_counter_with_labels() {
        let metrics = WorkerMetrics::new();
        metrics
            .jobs_completed
            .get_or_create(&JobTypeLabel("stage_plan".to_string()))
            .inc();
        metrics
            .jobs_completed
            .get_or_create(&JobTypeLabel("exec".to_string()))
            .inc();
        metrics
            .jobs_completed
            .get_or_create(&JobTypeLabel("exec".to_string()))
            .inc();

        let output = metrics.encode();
        assert!(
            output.contains("type=\"stage_plan\""),
            "Expected stage_plan label in output: {output}"
        );
        assert!(
            output.contains("type=\"exec\""),
            "Expected exec label in output: {output}"
        );
    }
}
