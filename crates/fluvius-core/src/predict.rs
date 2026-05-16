//! Predictive analytics — trajectory forecasting and anomaly detection.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::event::Event;

/// A point in a trajectory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrajectoryPoint {
    pub lon: f64,
    pub lat: f64,
    pub timestamp: DateTime<Utc>,
    pub speed: Option<f64>,
    pub heading: Option<f64>,
}

/// A tracked entity's trajectory.
#[derive(Debug, Clone)]
pub struct Trajectory {
    pub entity_id: String,
    pub points: Vec<TrajectoryPoint>,
    pub max_points: usize,
}

impl Trajectory {
    pub fn new(entity_id: String, max_points: usize) -> Self {
        Self {
            entity_id,
            points: Vec::new(),
            max_points,
        }
    }

    /// Add a point to the trajectory (FIFO eviction when full).
    pub fn add_point(&mut self, point: TrajectoryPoint) {
        if self.points.len() >= self.max_points {
            self.points.remove(0);
        }
        self.points.push(point);
    }

    /// Predict future position using linear extrapolation.
    pub fn predict_linear(&self, seconds_ahead: f64) -> Option<TrajectoryPoint> {
        let n = self.points.len();
        if n < 2 {
            return None;
        }

        let p1 = &self.points[n - 2];
        let p2 = &self.points[n - 1];

        let dt = (p2.timestamp - p1.timestamp).num_milliseconds() as f64 / 1000.0;
        if dt <= 0.0 {
            return None;
        }

        let vx = (p2.lon - p1.lon) / dt;
        let vy = (p2.lat - p1.lat) / dt;

        let speed = (vx * vx + vy * vy).sqrt() * 111_320.0; // approx m/s at equator
        let heading = vy.atan2(vx).to_degrees();

        Some(TrajectoryPoint {
            lon: p2.lon + vx * seconds_ahead,
            lat: p2.lat + vy * seconds_ahead,
            timestamp: p2.timestamp
                + chrono::Duration::milliseconds((seconds_ahead * 1000.0) as i64),
            speed: Some(speed),
            heading: Some(heading),
        })
    }

    /// Predict using weighted moving average of recent velocities.
    pub fn predict_weighted(&self, seconds_ahead: f64) -> Option<TrajectoryPoint> {
        let n = self.points.len();
        if n < 3 {
            return self.predict_linear(seconds_ahead);
        }

        let segments = n.min(6) - 1;
        let mut vx_sum = 0.0;
        let mut vy_sum = 0.0;
        let mut weight_sum = 0.0;

        for i in 0..segments {
            let idx = n - segments + i;
            let p1 = &self.points[idx - 1];
            let p2 = &self.points[idx];
            let dt = (p2.timestamp - p1.timestamp).num_milliseconds() as f64 / 1000.0;
            if dt <= 0.0 {
                continue;
            }

            let weight = (i as f64 + 1.0).powi(2);
            vx_sum += (p2.lon - p1.lon) / dt * weight;
            vy_sum += (p2.lat - p1.lat) / dt * weight;
            weight_sum += weight;
        }

        if weight_sum == 0.0 {
            return None;
        }

        let vx = vx_sum / weight_sum;
        let vy = vy_sum / weight_sum;
        let last = &self.points[n - 1];

        Some(TrajectoryPoint {
            lon: last.lon + vx * seconds_ahead,
            lat: last.lat + vy * seconds_ahead,
            timestamp: last.timestamp
                + chrono::Duration::milliseconds((seconds_ahead * 1000.0) as i64),
            speed: Some((vx * vx + vy * vy).sqrt() * 111_320.0),
            heading: Some(vy.atan2(vx).to_degrees()),
        })
    }

    /// Detect if the entity has deviated from predicted path (anomaly).
    pub fn is_anomalous(&self, threshold_meters: f64) -> bool {
        let n = self.points.len();
        if n < 3 {
            return false;
        }

        let prev_trajectory = Trajectory {
            entity_id: self.entity_id.clone(),
            points: self.points[..n - 1].to_vec(),
            max_points: self.max_points,
        };

        let actual = &self.points[n - 1];
        let dt =
            (actual.timestamp - self.points[n - 2].timestamp).num_milliseconds() as f64 / 1000.0;

        if let Some(predicted) = prev_trajectory.predict_linear(dt) {
            let dist = haversine_distance(actual.lat, actual.lon, predicted.lat, predicted.lon);
            dist > threshold_meters
        } else {
            false
        }
    }
}

/// Trajectory tracker — maintains trajectories for multiple entities.
pub struct TrajectoryTracker {
    trajectories: HashMap<String, Trajectory>,
    max_points: usize,
}

impl TrajectoryTracker {
    pub fn new(max_points: usize) -> Self {
        Self {
            trajectories: HashMap::new(),
            max_points,
        }
    }

    /// Update an entity's trajectory with a new event.
    pub fn update(&mut self, event: &Event) {
        let trajectory = self
            .trajectories
            .entry(event.entity_id.clone())
            .or_insert_with(|| Trajectory::new(event.entity_id.clone(), self.max_points));

        trajectory.add_point(TrajectoryPoint {
            lon: event.lon,
            lat: event.lat,
            timestamp: event.timestamp,
            speed: event.speed,
            heading: event.heading,
        });
    }

    /// Predict future position for an entity.
    pub fn predict(&self, entity_id: &str, seconds_ahead: f64) -> Option<TrajectoryPoint> {
        self.trajectories
            .get(entity_id)
            .and_then(|t| t.predict_weighted(seconds_ahead))
    }

    /// Get all anomalous entities.
    pub fn anomalous_entities(&self, threshold_meters: f64) -> Vec<&str> {
        self.trajectories
            .iter()
            .filter(|(_, t)| t.is_anomalous(threshold_meters))
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Number of tracked entities.
    pub fn entity_count(&self) -> usize {
        self.trajectories.len()
    }
}

/// Haversine distance in meters.
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let r = 6_371_000.0;
    let d_lat = (lat2 - lat1).to_radians();
    let d_lon = (lon2 - lon1).to_radians();
    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.to_radians().cos() * lat2.to_radians().cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    r * c
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn make_point(lon: f64, lat: f64, secs: i64) -> TrajectoryPoint {
        TrajectoryPoint {
            lon,
            lat,
            timestamp: Utc.timestamp_opt(1_700_000_000 + secs, 0).unwrap(),
            speed: None,
            heading: None,
        }
    }

    #[test]
    fn test_linear_prediction() {
        let mut traj = Trajectory::new("ship1".to_string(), 100);
        traj.add_point(make_point(0.0, 0.0, 0));
        traj.add_point(make_point(0.001, 0.0, 10));

        let predicted = traj.predict_linear(10.0).unwrap();
        assert!((predicted.lon - 0.002).abs() < 1e-6);
    }

    #[test]
    fn test_weighted_prediction() {
        let mut traj = Trajectory::new("ship1".to_string(), 100);
        traj.add_point(make_point(0.0, 0.0, 0));
        traj.add_point(make_point(0.001, 0.0, 10));
        traj.add_point(make_point(0.002, 0.0, 20));
        traj.add_point(make_point(0.003, 0.0, 30));

        let predicted = traj.predict_weighted(10.0).unwrap();
        assert!((predicted.lon - 0.004).abs() < 0.001);
    }

    #[test]
    fn test_anomaly_detection() {
        let mut traj = Trajectory::new("ship1".to_string(), 100);
        traj.add_point(make_point(0.0, 0.0, 0));
        traj.add_point(make_point(0.001, 0.0, 10));
        traj.add_point(make_point(0.002, 0.0, 20));
        traj.add_point(make_point(0.1, 0.0, 30)); // Sudden jump
        assert!(traj.is_anomalous(100.0));
    }

    #[test]
    fn test_tracker() {
        let mut tracker = TrajectoryTracker::new(50);
        let event1 = Event::new(
            "vessel-1",
            1.0,
            51.0,
            Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
        );
        let event2 = Event::new(
            "vessel-1",
            1.001,
            51.0,
            Utc.timestamp_opt(1_700_000_010, 0).unwrap(),
        );

        tracker.update(&event1);
        tracker.update(&event2);

        assert_eq!(tracker.entity_count(), 1);
        let pred = tracker.predict("vessel-1", 10.0);
        assert!(pred.is_some());
    }
}
