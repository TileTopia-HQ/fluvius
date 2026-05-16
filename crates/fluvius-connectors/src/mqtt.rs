//! MQTT connector — publish/subscribe events via MQTT (IoT protocol).

use fluvius_core::event::Event;
use std::time::Duration;
use tokio::sync::mpsc;

/// MQTT connection configuration.
#[derive(Debug, Clone)]
pub struct MqttConfig {
    pub broker_url: String,
    pub topic: String,
    pub client_id: String,
    pub qos: MqttQos,
    pub keep_alive: Duration,
    pub username: Option<String>,
    pub password: Option<String>,
}

/// MQTT Quality of Service levels.
#[derive(Debug, Clone, Copy)]
pub enum MqttQos {
    /// At most once (fire and forget).
    AtMostOnce,
    /// At least once (acknowledged delivery).
    AtLeastOnce,
    /// Exactly once (assured delivery).
    ExactlyOnce,
}

impl MqttConfig {
    pub fn new(broker_url: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            topic: topic.into(),
            client_id: "fluvius-mqtt".to_string(),
            qos: MqttQos::AtLeastOnce,
            keep_alive: Duration::from_secs(30),
            username: None,
            password: None,
        }
    }

    pub fn with_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }

    pub fn with_qos(mut self, qos: MqttQos) -> Self {
        self.qos = qos;
        self
    }
}

/// MQTT subscriber source — reads events from an MQTT topic.
pub struct MqttSource {
    config: MqttConfig,
}

impl MqttSource {
    pub fn new(config: MqttConfig) -> Self {
        Self { config }
    }

    /// Start subscribing to events.
    /// NOTE: Skeleton — add `rumqttc` dependency for production use.
    pub async fn start(&self, _sender: mpsc::Sender<Event>) -> Result<(), MqttError> {
        Err(MqttError::NotConfigured(format!(
            "MQTT subscriber for topic '{}' — add rumqttc dependency for production use",
            self.config.topic
        )))
    }

    pub fn config(&self) -> &MqttConfig {
        &self.config
    }
}

/// MQTT publisher sink — publishes events to an MQTT topic.
pub struct MqttSink {
    config: MqttConfig,
}

impl MqttSink {
    pub fn new(config: MqttConfig) -> Self {
        Self { config }
    }

    /// Publish an event.
    pub async fn send(&self, _event: &Event) -> Result<(), MqttError> {
        Err(MqttError::NotConfigured(format!(
            "MQTT publisher for topic '{}' — add rumqttc dependency for production use",
            self.config.topic
        )))
    }

    pub fn config(&self) -> &MqttConfig {
        &self.config
    }
}

/// MQTT-related errors.
#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    #[error("MQTT not configured: {0}")]
    NotConfigured(String),
    #[error("MQTT connection error: {0}")]
    Connection(String),
    #[error("MQTT publish error: {0}")]
    Publish(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_config() {
        let config = MqttConfig::new("mqtt://localhost:1883", "sensors/gps")
            .with_credentials("user", "pass")
            .with_qos(MqttQos::ExactlyOnce);
        assert_eq!(config.topic, "sensors/gps");
        assert!(config.username.is_some());
    }

    #[tokio::test]
    async fn test_mqtt_source_not_configured() {
        let config = MqttConfig::new("mqtt://localhost:1883", "test");
        let source = MqttSource::new(config);
        let (tx, _rx) = mpsc::channel(10);
        assert!(source.start(tx).await.is_err());
    }
}
