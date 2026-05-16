//! Edge runtime — lightweight deployment for constrained environments.
//!
//! Provides a minimal stream processor that can run on ARM/embedded devices
//! with limited memory and no external dependencies.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::event::{Event, OutputEvent};
use crate::operator::MapOperator;

/// Edge runtime configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeConfig {
    /// Maximum events to buffer in memory.
    pub max_buffer_size: usize,
    /// Flush interval for batch processing.
    pub flush_interval_ms: u64,
    /// Maximum memory usage in bytes (soft limit).
    pub max_memory_bytes: usize,
    /// Whether to enable local persistence (store-and-forward).
    pub store_and_forward: bool,
    /// Batch size for upstream sync.
    pub sync_batch_size: usize,
}

impl Default for EdgeConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 1000,
            flush_interval_ms: 5000,
            max_memory_bytes: 64 * 1024 * 1024,
            store_and_forward: true,
            sync_batch_size: 100,
        }
    }
}

/// Edge runtime metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EdgeMetrics {
    pub events_received: u64,
    pub events_processed: u64,
    pub events_forwarded: u64,
    pub events_dropped: u64,
    pub buffer_size: usize,
}

/// Lightweight edge stream processor.
pub struct EdgeRuntime {
    config: EdgeConfig,
    buffer: VecDeque<Event>,
    forward_queue: VecDeque<OutputEvent>,
    operators: Vec<Box<dyn MapOperator>>,
    metrics: EdgeMetrics,
    last_flush: Instant,
}

impl EdgeRuntime {
    pub fn new(config: EdgeConfig) -> Self {
        Self {
            config,
            buffer: VecDeque::new(),
            forward_queue: VecDeque::new(),
            operators: Vec::new(),
            metrics: EdgeMetrics::default(),
            last_flush: Instant::now(),
        }
    }

    /// Add an operator to the edge pipeline.
    pub fn add_operator(&mut self, op: Box<dyn MapOperator>) {
        self.operators.push(op);
    }

    /// Ingest a single event.
    pub fn ingest(&mut self, event: Event) {
        self.metrics.events_received += 1;

        if self.buffer.len() >= self.config.max_buffer_size {
            self.buffer.pop_front();
            self.metrics.events_dropped += 1;
        }

        self.buffer.push_back(event);
        self.metrics.buffer_size = self.buffer.len();
    }

    /// Process buffered events through the operator chain.
    pub fn process(&mut self) -> Vec<OutputEvent> {
        let mut results = Vec::new();

        while let Some(event) = self.buffer.pop_front() {
            let mut outputs = Vec::new();

            // Run through operator chain
            let mut passed = true;
            for op in &self.operators {
                if let Some(output) = op.process(&event) {
                    outputs.push(output);
                } else {
                    passed = false;
                    break;
                }
            }

            // If no operators or all passed, produce output
            if passed && outputs.is_empty() {
                outputs.push(OutputEvent {
                    source_event: event.clone(),
                    operator: "passthrough".to_string(),
                    payload: serde_json::Value::Null,
                });
            }

            self.metrics.events_processed += 1;

            if self.config.store_and_forward {
                for output in &outputs {
                    self.forward_queue.push_back(output.clone());
                }
            }

            results.extend(outputs);
        }

        self.metrics.buffer_size = self.buffer.len();
        results
    }

    /// Check if it's time to flush.
    pub fn should_flush(&self) -> bool {
        self.last_flush.elapsed() >= Duration::from_millis(self.config.flush_interval_ms)
            || self.forward_queue.len() >= self.config.sync_batch_size
    }

    /// Get a batch of events to forward upstream.
    pub fn drain_forward_queue(&mut self) -> Vec<OutputEvent> {
        let batch_size = self.config.sync_batch_size.min(self.forward_queue.len());
        let batch: Vec<OutputEvent> = self.forward_queue.drain(..batch_size).collect();
        self.metrics.events_forwarded += batch.len() as u64;
        self.last_flush = Instant::now();
        batch
    }

    /// Get current metrics.
    pub fn metrics(&self) -> &EdgeMetrics {
        &self.metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_edge_ingest() {
        let mut runtime = EdgeRuntime::new(EdgeConfig::default());
        runtime.ingest(Event::new("e1", 1.0, 51.0, Utc::now()));
        runtime.ingest(Event::new("e2", 1.0, 51.0, Utc::now()));
        assert_eq!(runtime.metrics().events_received, 2);
        assert_eq!(runtime.metrics().buffer_size, 2);
    }

    #[test]
    fn test_edge_process() {
        let mut runtime = EdgeRuntime::new(EdgeConfig::default());
        runtime.ingest(Event::new("e1", 1.0, 51.0, Utc::now()));
        runtime.ingest(Event::new("e2", 1.0, 51.0, Utc::now()));

        let results = runtime.process();
        assert_eq!(results.len(), 2);
        assert_eq!(runtime.metrics().events_processed, 2);
    }

    #[test]
    fn test_edge_buffer_overflow() {
        let config = EdgeConfig {
            max_buffer_size: 3,
            ..Default::default()
        };
        let mut runtime = EdgeRuntime::new(config);

        for _ in 0..5 {
            runtime.ingest(Event::new("e", 1.0, 51.0, Utc::now()));
        }

        assert_eq!(runtime.metrics().events_dropped, 2);
        assert_eq!(runtime.metrics().buffer_size, 3);
    }

    #[test]
    fn test_edge_forward_queue() {
        let config = EdgeConfig {
            sync_batch_size: 2,
            store_and_forward: true,
            ..Default::default()
        };
        let mut runtime = EdgeRuntime::new(config);
        runtime.ingest(Event::new("e1", 1.0, 51.0, Utc::now()));
        runtime.ingest(Event::new("e2", 1.0, 51.0, Utc::now()));
        runtime.ingest(Event::new("e3", 1.0, 51.0, Utc::now()));

        runtime.process();
        let batch = runtime.drain_forward_queue();
        assert_eq!(batch.len(), 2);
        assert_eq!(runtime.metrics().events_forwarded, 2);
    }
}
