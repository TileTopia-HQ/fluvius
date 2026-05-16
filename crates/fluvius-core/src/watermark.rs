//! Watermark tracking for event-time progress.

use chrono::{DateTime, Duration, Utc};

/// Tracks the progress of event time across a stream.
/// Events with timestamps older than the watermark are considered late.
pub struct Watermark {
    /// Current watermark position.
    current: DateTime<Utc>,
    /// Maximum allowed lateness before events are dropped.
    max_lateness: Duration,
    /// How many late events have been observed.
    late_count: u64,
}

impl Watermark {
    /// Create a new watermark with the given max lateness tolerance.
    pub fn new(max_lateness: Duration) -> Self {
        Self {
            current: DateTime::from_timestamp(0, 0).unwrap(),
            max_lateness,
            late_count: 0,
        }
    }

    /// Advance the watermark based on an observed event timestamp.
    /// Returns true if the event is on-time, false if late.
    pub fn advance(&mut self, event_time: &DateTime<Utc>) -> bool {
        if *event_time >= self.current {
            // Advance watermark (with lateness buffer)
            self.current = *event_time - self.max_lateness;
            true
        } else if *event_time >= self.current - self.max_lateness {
            // Late but within tolerance
            self.late_count += 1;
            true
        } else {
            // Too late, beyond tolerance
            self.late_count += 1;
            false
        }
    }

    /// Get the current watermark position.
    pub fn current(&self) -> &DateTime<Utc> {
        &self.current
    }

    /// Get the count of late events observed.
    pub fn late_count(&self) -> u64 {
        self.late_count
    }

    /// Check if a timestamp is considered late relative to current watermark.
    pub fn is_late(&self, ts: &DateTime<Utc>) -> bool {
        *ts < self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_advance() {
        let mut wm = Watermark::new(Duration::seconds(5));
        let ts1 = DateTime::from_timestamp(100, 0).unwrap();
        let ts2 = DateTime::from_timestamp(110, 0).unwrap();

        assert!(wm.advance(&ts1));
        assert!(wm.advance(&ts2));
        // Watermark should be at ts2 - 5s = 105
        assert_eq!(*wm.current(), DateTime::from_timestamp(105, 0).unwrap());
    }

    #[test]
    fn test_late_events() {
        let mut wm = Watermark::new(Duration::seconds(2));
        let ts1 = DateTime::from_timestamp(100, 0).unwrap();
        let ts2 = DateTime::from_timestamp(90, 0).unwrap(); // Very late

        wm.advance(&ts1);
        let accepted = wm.advance(&ts2);
        assert!(!accepted); // Too late (ts2=90, watermark=98, tolerance window is 96..98)
        assert_eq!(wm.late_count(), 1);
    }

    #[test]
    fn test_within_lateness_tolerance() {
        let mut wm = Watermark::new(Duration::seconds(10));
        let ts1 = DateTime::from_timestamp(100, 0).unwrap();
        let ts2 = DateTime::from_timestamp(88, 0).unwrap(); // Late but within tolerance

        wm.advance(&ts1); // watermark = 90
        let accepted = wm.advance(&ts2); // 88 >= 90 - 10 = 80, so within tolerance
        assert!(accepted);
        assert_eq!(wm.late_count(), 1);
    }
}
