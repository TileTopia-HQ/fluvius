//! Stream operators — the processing units in a pipeline.

use crate::event::{Event, OutputEvent};

/// Trait for stateless stream operators that process one event at a time.
pub trait MapOperator: Send + Sync {
    /// Process a single event, optionally producing an output.
    fn process(&self, event: &Event) -> Option<OutputEvent>;

    /// Operator name for logging/metrics.
    fn name(&self) -> &str;
}

/// Trait for stateful operators that may buffer events.
pub trait StatefulOperator: Send + Sync {
    /// Process an event, potentially updating internal state.
    /// May produce zero or more outputs.
    fn process(&mut self, event: &Event) -> Vec<OutputEvent>;

    /// Called when a window expires — flush any buffered state.
    fn on_window_close(&mut self) -> Vec<OutputEvent>;

    /// Operator name.
    fn name(&self) -> &str;
}

/// A filter operator that passes through events matching a predicate.
pub struct FilterOperator {
    name: String,
    predicate: Box<dyn Fn(&Event) -> bool + Send + Sync>,
}

impl FilterOperator {
    pub fn new(
        name: impl Into<String>,
        predicate: impl Fn(&Event) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            predicate: Box::new(predicate),
        }
    }
}

impl MapOperator for FilterOperator {
    fn process(&self, event: &Event) -> Option<OutputEvent> {
        if (self.predicate)(event) {
            Some(OutputEvent {
                source_event: event.clone(),
                operator: self.name.clone(),
                payload: serde_json::json!({"action": "pass"}),
            })
        } else {
            None
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A transform operator that modifies events.
pub struct TransformOperator {
    name: String,
    transform: Box<dyn Fn(&Event) -> Event + Send + Sync>,
}

impl TransformOperator {
    pub fn new(
        name: impl Into<String>,
        transform: impl Fn(&Event) -> Event + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            transform: Box::new(transform),
        }
    }
}

impl MapOperator for TransformOperator {
    fn process(&self, event: &Event) -> Option<OutputEvent> {
        let transformed = (self.transform)(event);
        Some(OutputEvent {
            source_event: transformed,
            operator: self.name.clone(),
            payload: serde_json::json!({"action": "transform"}),
        })
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// Rate limiter — emits at most N events per entity per window.
pub struct RateLimiter {
    name: String,
    max_per_entity: usize,
    counts: std::collections::HashMap<String, usize>,
}

impl RateLimiter {
    pub fn new(name: impl Into<String>, max_per_entity: usize) -> Self {
        Self {
            name: name.into(),
            max_per_entity,
            counts: std::collections::HashMap::new(),
        }
    }
}

impl StatefulOperator for RateLimiter {
    fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
        let count = self.counts.entry(event.entity_id.clone()).or_insert(0);
        if *count < self.max_per_entity {
            *count += 1;
            vec![OutputEvent {
                source_event: event.clone(),
                operator: self.name.clone(),
                payload: serde_json::json!({"action": "pass", "count": *count}),
            }]
        } else {
            vec![]
        }
    }

    fn on_window_close(&mut self) -> Vec<OutputEvent> {
        self.counts.clear();
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_operator() {
        let filter = FilterOperator::new("speed_filter", |e: &Event| e.speed.unwrap_or(0.0) > 10.0);

        let fast = Event::now("v1", 0.0, 0.0).with_speed(20.0);
        let slow = Event::now("v2", 0.0, 0.0).with_speed(5.0);

        assert!(filter.process(&fast).is_some());
        assert!(filter.process(&slow).is_none());
    }

    #[test]
    fn test_transform_operator() {
        let transform = TransformOperator::new("double_speed", |e: &Event| {
            let mut cloned = e.clone();
            cloned.speed = cloned.speed.map(|s| s * 2.0);
            cloned
        });

        let event = Event::now("v1", 0.0, 0.0).with_speed(10.0);
        let output = transform.process(&event).unwrap();
        assert_eq!(output.source_event.speed, Some(20.0));
    }

    #[test]
    fn test_rate_limiter() {
        let mut limiter = RateLimiter::new("limiter", 2);

        let e1 = Event::now("v1", 0.0, 0.0);
        let e2 = Event::now("v1", 1.0, 1.0);
        let e3 = Event::now("v1", 2.0, 2.0);
        let e4 = Event::now("v2", 3.0, 3.0);

        assert_eq!(limiter.process(&e1).len(), 1);
        assert_eq!(limiter.process(&e2).len(), 1);
        assert_eq!(limiter.process(&e3).len(), 0); // Rate limited
        assert_eq!(limiter.process(&e4).len(), 1); // Different entity

        // After window close, counts reset
        limiter.on_window_close();
        assert_eq!(limiter.process(&e1).len(), 1); // v1 can send again
    }
}
