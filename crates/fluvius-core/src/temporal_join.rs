//! Multi-stream temporal joins — correlate events across sources by time proximity.

use std::collections::VecDeque;
use std::time::Duration;

use chrono::{DateTime, Utc};

use crate::event::{Event, OutputEvent};

/// Join semantics.
#[derive(Debug, Clone, Copy)]
pub enum JoinType {
    /// Both sides must match within the window.
    Inner,
    /// Left side always emits; right side optional.
    LeftOuter,
}

/// A temporal join operator that correlates events from two streams
/// based on entity_id and temporal proximity.
pub struct TemporalJoin {
    name: String,
    window: Duration,
    join_type: JoinType,
    /// Buffered left-side events waiting for a match.
    left_buffer: VecDeque<Event>,
    /// Buffered right-side events waiting for a match.
    right_buffer: VecDeque<Event>,
}

impl TemporalJoin {
    pub fn new(name: impl Into<String>, window: Duration, join_type: JoinType) -> Self {
        Self {
            name: name.into(),
            window,
            join_type,
            left_buffer: VecDeque::new(),
            right_buffer: VecDeque::new(),
        }
    }

    /// Add an event from the left stream.
    pub fn push_left(&mut self, event: Event) -> Vec<OutputEvent> {
        self.expire_old(event.timestamp);
        let matches = self.find_matches(&event, &self.right_buffer);

        if matches.is_empty() {
            self.left_buffer.push_back(event);
            Vec::new()
        } else {
            matches
                .into_iter()
                .map(|right| self.emit_join(&event, Some(&right)))
                .collect()
        }
    }

    /// Add an event from the right stream.
    pub fn push_right(&mut self, event: Event) -> Vec<OutputEvent> {
        self.expire_old(event.timestamp);
        let matches = self.find_matches(&event, &self.left_buffer);

        if matches.is_empty() {
            self.right_buffer.push_back(event);
            Vec::new()
        } else {
            matches
                .into_iter()
                .map(|left| self.emit_join(&left, Some(&event)))
                .collect()
        }
    }

    /// Flush expired left-side events (for left-outer joins).
    pub fn flush_expired(&mut self, now: DateTime<Utc>) -> Vec<OutputEvent> {
        if !matches!(self.join_type, JoinType::LeftOuter) {
            return Vec::new();
        }

        let mut outputs = Vec::new();
        while let Some(front) = self.left_buffer.front() {
            let age = now.signed_duration_since(front.timestamp);
            if age.num_milliseconds() > self.window.as_millis() as i64 {
                let expired = self.left_buffer.pop_front().unwrap();
                outputs.push(self.emit_join(&expired, None));
            } else {
                break;
            }
        }
        outputs
    }

    fn find_matches(&self, event: &Event, buffer: &VecDeque<Event>) -> Vec<Event> {
        buffer
            .iter()
            .filter(|candidate| {
                candidate.entity_id == event.entity_id
                    && (event
                        .timestamp
                        .signed_duration_since(candidate.timestamp)
                        .num_milliseconds()
                        .unsigned_abs()
                        <= self.window.as_millis() as u64)
            })
            .cloned()
            .collect()
    }

    fn expire_old(&mut self, now: DateTime<Utc>) {
        let window_ms = self.window.as_millis() as i64 * 2; // Keep 2x window for safety
        self.left_buffer
            .retain(|e| now.signed_duration_since(e.timestamp).num_milliseconds() <= window_ms);
        self.right_buffer
            .retain(|e| now.signed_duration_since(e.timestamp).num_milliseconds() <= window_ms);
    }

    fn emit_join(&self, left: &Event, right: Option<&Event>) -> OutputEvent {
        OutputEvent {
            source_event: left.clone(),
            operator: self.name.clone(),
            payload: serde_json::json!({
                "join_type": format!("{:?}", self.join_type),
                "left_entity": left.entity_id,
                "left_time": left.timestamp.to_rfc3339(),
                "right": right.map(|r| serde_json::json!({
                    "entity": &r.entity_id,
                    "time": r.timestamp.to_rfc3339(),
                    "lon": r.lon,
                    "lat": r.lat,
                })),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    #[test]
    fn test_inner_join_match() {
        let mut join = TemporalJoin::new("test", Duration::from_secs(5), JoinType::Inner);
        let now = Utc::now();

        let left = Event::new("v1", 0.0, 0.0, now);
        let right = Event::new("v1", 1.0, 1.0, now + TimeDelta::seconds(2));

        let r1 = join.push_left(left);
        assert!(r1.is_empty()); // No match yet

        let r2 = join.push_right(right);
        assert_eq!(r2.len(), 1); // Matched!
    }

    #[test]
    fn test_inner_join_no_match_different_entity() {
        let mut join = TemporalJoin::new("test", Duration::from_secs(5), JoinType::Inner);
        let now = Utc::now();

        let left = Event::new("v1", 0.0, 0.0, now);
        let right = Event::new("v2", 1.0, 1.0, now + TimeDelta::seconds(2));

        join.push_left(left);
        let r = join.push_right(right);
        assert!(r.is_empty()); // Different entities
    }

    #[test]
    fn test_left_outer_flush() {
        let mut join = TemporalJoin::new("test", Duration::from_secs(5), JoinType::LeftOuter);
        let now = Utc::now();

        let left = Event::new("v1", 0.0, 0.0, now);
        join.push_left(left);

        // Flush after window expires
        let expired = join.flush_expired(now + TimeDelta::seconds(10));
        assert_eq!(expired.len(), 1);
    }
}
