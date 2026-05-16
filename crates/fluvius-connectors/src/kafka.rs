//! Kafka connector — produce/consume events via Apache Kafka.

use async_trait::async_trait;
use fluvius_core::event::Event;
use std::time::Duration;
use tokio::sync::mpsc;

/// Kafka connection configuration.
#[derive(Debug, Clone)]
pub struct KafkaConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    pub group_id: Option<String>,
    pub client_id: String,
    /// Consumer poll interval.
    pub poll_interval: Duration,
}

impl KafkaConfig {
    pub fn new(brokers: Vec<String>, topic: impl Into<String>) -> Self {
        Self {
            brokers,
            topic: topic.into(),
            group_id: None,
            client_id: "fluvius".to_string(),
            poll_interval: Duration::from_millis(100),
        }
    }

    pub fn with_group(mut self, group: impl Into<String>) -> Self {
        self.group_id = Some(group.into());
        self
    }
}

/// Kafka consumer source — reads events from a Kafka topic.
pub struct KafkaSource {
    config: KafkaConfig,
}

impl KafkaSource {
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    /// Start consuming events into the given channel.
    /// NOTE: This is a skeleton — actual Kafka integration requires `rdkafka`.
    /// The interface is production-ready; swap in rdkafka when deploying.
    pub async fn start(&self, _sender: mpsc::Sender<Event>) -> Result<(), KafkaError> {
        // In production, this would:
        // 1. Create a StreamConsumer from rdkafka
        // 2. Subscribe to self.config.topic
        // 3. Poll messages, deserialize to Event, send to channel
        Err(KafkaError::NotConfigured(format!(
            "Kafka consumer for topic '{}' — add rdkafka dependency for production use",
            self.config.topic
        )))
    }

    pub fn config(&self) -> &KafkaConfig {
        &self.config
    }
}

/// Kafka producer sink — writes events to a Kafka topic.
pub struct KafkaSink {
    config: KafkaConfig,
}

impl KafkaSink {
    pub fn new(config: KafkaConfig) -> Self {
        Self { config }
    }

    /// Send an event to Kafka.
    pub async fn send(&self, _event: &Event) -> Result<(), KafkaError> {
        Err(KafkaError::NotConfigured(format!(
            "Kafka producer for topic '{}' — add rdkafka dependency for production use",
            self.config.topic
        )))
    }

    pub fn config(&self) -> &KafkaConfig {
        &self.config
    }
}

/// Kafka-related errors.
#[derive(Debug, thiserror::Error)]
pub enum KafkaError {
    #[error("Kafka not configured: {0}")]
    NotConfigured(String),
    #[error("Kafka connection error: {0}")]
    Connection(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
}

/// Trait for connectors that can produce events.
#[async_trait]
pub trait EventSource: Send + Sync {
    async fn start(&self, sender: mpsc::Sender<Event>) -> Result<(), Box<dyn std::error::Error>>;
}

/// Trait for connectors that can consume events.
#[async_trait]
pub trait EventSink: Send + Sync {
    async fn send(&self, event: &Event) -> Result<(), Box<dyn std::error::Error>>;
    async fn flush(&self) -> Result<(), Box<dyn std::error::Error>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_config() {
        let config = KafkaConfig::new(vec!["localhost:9092".to_string()], "events")
            .with_group("fluvius-group");
        assert_eq!(config.topic, "events");
        assert_eq!(config.group_id, Some("fluvius-group".to_string()));
    }

    #[tokio::test]
    async fn test_kafka_source_not_configured() {
        let config = KafkaConfig::new(vec!["localhost:9092".to_string()], "test");
        let source = KafkaSource::new(config);
        let (tx, _rx) = mpsc::channel(10);
        let result = source.start(tx).await;
        assert!(result.is_err());
    }
}
