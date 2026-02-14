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
