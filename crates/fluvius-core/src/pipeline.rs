//! Stream processing pipeline — connects sources, operators, and sinks.

use std::sync::Arc;
use tokio::sync::mpsc;

use crate::event::{Event, OutputEvent};
use crate::operator::MapOperator;

/// A processing pipeline that connects a source channel to operators and a sink.
pub struct Pipeline {
    name: String,
    operators: Vec<Arc<dyn MapOperator>>,
}

/// Pipeline execution metrics.
#[derive(Debug, Clone, Default)]
pub struct PipelineMetrics {
    pub events_received: u64,
    pub events_emitted: u64,
    pub events_filtered: u64,
}

impl Pipeline {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            operators: Vec::new(),
        }
    }

    /// Add an operator to the pipeline.
    pub fn add_operator(&mut self, op: Arc<dyn MapOperator>) {
        self.operators.push(op);
    }

    /// Get pipeline name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Run the pipeline, processing events from the receiver and sending outputs to the sender.
    pub async fn run(
        &self,
        mut input: mpsc::Receiver<Event>,
        output: mpsc::Sender<OutputEvent>,
    ) -> PipelineMetrics {
        let mut metrics = PipelineMetrics::default();

        while let Some(event) = input.recv().await {
            metrics.events_received += 1;

            // Pass event through operator chain
            let mut current_event = Some(event);

            for op in &self.operators {
                if let Some(ref evt) = current_event
                    && let Some(out) = op.process(evt)
                {
                    current_event = Some(out.source_event.clone());
                    if output.send(out).await.is_err() {
                        return metrics;
                    }
                    metrics.events_emitted += 1;
                } else if current_event.is_some() {
                    metrics.events_filtered += 1;
                    break;
                }
            }
        }

        metrics
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operator::FilterOperator;

    #[tokio::test]
    async fn test_pipeline_basic() {
        let mut pipeline = Pipeline::new("test");
        pipeline.add_operator(Arc::new(FilterOperator::new("pass_all", |_| true)));

        let (tx_in, rx_in) = mpsc::channel(100);
        let (tx_out, mut rx_out) = mpsc::channel(100);

        // Send events
        let e1 = Event::now("v1", 0.0, 0.0);
        let e2 = Event::now("v2", 1.0, 1.0);
        tx_in.send(e1).await.unwrap();
        tx_in.send(e2).await.unwrap();
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(metrics.events_received, 2);
        assert_eq!(metrics.events_emitted, 2);

        let out1 = rx_out.recv().await.unwrap();
        assert_eq!(out1.source_event.entity_id, "v1");
    }

    #[tokio::test]
    async fn test_pipeline_with_filter() {
        let mut pipeline = Pipeline::new("filtered");
        pipeline.add_operator(Arc::new(FilterOperator::new("fast_only", |e: &Event| {
            e.speed.unwrap_or(0.0) > 10.0
        })));

        let (tx_in, rx_in) = mpsc::channel(100);
        let (tx_out, mut rx_out) = mpsc::channel(100);

        tx_in
            .send(Event::now("v1", 0.0, 0.0).with_speed(20.0))
            .await
            .unwrap();
        tx_in
            .send(Event::now("v2", 0.0, 0.0).with_speed(5.0))
            .await
            .unwrap();
        tx_in
            .send(Event::now("v3", 0.0, 0.0).with_speed(30.0))
            .await
            .unwrap();
        drop(tx_in);

        let metrics = pipeline.run(rx_in, tx_out).await;
        assert_eq!(metrics.events_received, 3);
        assert_eq!(metrics.events_emitted, 2);
        assert_eq!(metrics.events_filtered, 1);

        let out1 = rx_out.recv().await.unwrap();
        assert_eq!(out1.source_event.entity_id, "v1");
        let out2 = rx_out.recv().await.unwrap();
        assert_eq!(out2.source_event.entity_id, "v3");
    }
}
