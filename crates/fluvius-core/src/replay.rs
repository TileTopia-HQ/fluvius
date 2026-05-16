//! Replay mode — replay historical data at configurable speed.

use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::event::Event;

/// Replay speed configuration.
#[derive(Debug, Clone, Copy)]
pub enum ReplaySpeed {
    /// Real-time (1x).
    RealTime,
    /// Multiplied speed (e.g., 10.0 = 10x faster).
    Multiplied(f64),
    /// As fast as possible (no delays).
    MaxSpeed,
}

/// Replays a sequence of events respecting their timestamps.
pub struct Replayer {
    speed: ReplaySpeed,
}

impl Replayer {
    pub fn new(speed: ReplaySpeed) -> Self {
        Self { speed }
    }

    /// Replay events into the given sender, respecting inter-event timing.
    pub async fn replay(&self, events: Vec<Event>, sender: mpsc::Sender<Event>) -> ReplayStats {
        let mut stats = ReplayStats {
            events_replayed: 0,
            wall_time_ms: 0,
            original_duration_ms: 0,
        };

        if events.is_empty() {
            return stats;
        }

        let start_wall = tokio::time::Instant::now();
        let first_ts = events[0].timestamp;
        let last_ts = events.last().unwrap().timestamp;
        stats.original_duration_ms = last_ts
            .signed_duration_since(first_ts)
            .num_milliseconds()
            .unsigned_abs();

        let mut prev_ts: Option<DateTime<Utc>> = None;

        for event in events {
            if let Some(prev) = prev_ts {
                let gap = event
                    .timestamp
                    .signed_duration_since(prev)
                    .num_milliseconds()
                    .max(0) as u64;

                let delay = match self.speed {
                    ReplaySpeed::RealTime => Duration::from_millis(gap),
                    ReplaySpeed::Multiplied(factor) => {
                        Duration::from_millis((gap as f64 / factor) as u64)
                    }
                    ReplaySpeed::MaxSpeed => Duration::ZERO,
                };

                if !delay.is_zero() {
                    sleep(delay).await;
                }
            }

            prev_ts = Some(event.timestamp);

            if sender.send(event).await.is_err() {
                break;
            }
            stats.events_replayed += 1;
        }

        stats.wall_time_ms = start_wall.elapsed().as_millis() as u64;
        stats
    }
}

/// Statistics from a replay session.
#[derive(Debug, Clone)]
pub struct ReplayStats {
    pub events_replayed: u64,
    pub wall_time_ms: u64,
    pub original_duration_ms: u64,
}

impl ReplayStats {
    pub fn effective_speed(&self) -> f64 {
        if self.wall_time_ms == 0 {
            return f64::INFINITY;
        }
        self.original_duration_ms as f64 / self.wall_time_ms as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    #[tokio::test]
    async fn test_replay_max_speed() {
        let replayer = Replayer::new(ReplaySpeed::MaxSpeed);
        let (tx, mut rx) = mpsc::channel(100);

        let now = Utc::now();
        let events = vec![
            Event::new("v1", 0.0, 0.0, now),
            Event::new("v1", 1.0, 1.0, now + TimeDelta::seconds(60)),
            Event::new("v1", 2.0, 2.0, now + TimeDelta::seconds(120)),
        ];

        let stats = replayer.replay(events, tx).await;
        assert_eq!(stats.events_replayed, 3);
        assert!(stats.wall_time_ms < 100); // Should be near-instant
        assert_eq!(stats.original_duration_ms, 120_000);

        let mut received = Vec::new();
        while let Ok(e) = rx.try_recv() {
            received.push(e);
        }
        assert_eq!(received.len(), 3);
    }

    #[tokio::test]
    async fn test_replay_multiplied() {
        let replayer = Replayer::new(ReplaySpeed::Multiplied(100.0));
        let (tx, _rx) = mpsc::channel(100);

        let now = Utc::now();
        let events = vec![
            Event::new("v1", 0.0, 0.0, now),
            Event::new("v1", 1.0, 1.0, now + TimeDelta::seconds(1)),
        ];

        let stats = replayer.replay(events, tx).await;
        assert_eq!(stats.events_replayed, 2);
        // 1s gap at 100x = 10ms delay — should finish quickly
        assert!(stats.wall_time_ms < 50);
    }
}
