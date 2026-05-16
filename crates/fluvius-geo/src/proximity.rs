//! Proximity detection — distance-based alerts between entities.

use std::collections::HashMap;

use fluvius_core::event::{Event, OutputEvent};
use fluvius_core::operator::StatefulOperator;
use geo::Point;
use geo::algorithm::line_measures::{Distance, Haversine};

/// Proximity operator — emits alerts when entities come within a threshold distance.
pub struct ProximityOperator {
    name: String,
    /// Distance threshold in meters.
    threshold_meters: f64,
    /// Last known position per entity.
    positions: HashMap<String, (f64, f64)>,
}

impl ProximityOperator {
    pub fn new(name: impl Into<String>, threshold_meters: f64) -> Self {
        Self {
            name: name.into(),
            threshold_meters,
            positions: HashMap::new(),
        }
    }
}

impl StatefulOperator for ProximityOperator {
    fn process(&mut self, event: &Event) -> Vec<OutputEvent> {
        let mut outputs = Vec::new();
        let event_point = Point::new(event.lon, event.lat);

        // Check distance to all other known entities
        for (other_id, (lon, lat)) in &self.positions {
            if *other_id == event.entity_id {
                continue;
            }
            let other_point = Point::new(*lon, *lat);
            let distance = Haversine::distance(event_point, other_point);

            if distance <= self.threshold_meters {
                outputs.push(OutputEvent {
                    source_event: event.clone(),
                    operator: self.name.clone(),
                    payload: serde_json::json!({
                        "alert": "proximity",
                        "entity_a": event.entity_id,
                        "entity_b": other_id,
                        "distance_meters": distance,
                        "threshold_meters": self.threshold_meters,
                    }),
                });
            }
        }

        // Update this entity's position
        self.positions
            .insert(event.entity_id.clone(), (event.lon, event.lat));

        outputs
    }

    fn on_window_close(&mut self) -> Vec<OutputEvent> {
        // Clear stale positions on window close
        self.positions.clear();
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
    fn test_proximity_alert() {
        let mut op = ProximityOperator::new("proximity", 1000.0); // 1km threshold

        // Vehicle 1 at a position
        let e1 = Event::now("v1", -73.9857, 40.7484); // NYC
        let out1 = op.process(&e1);
        assert!(out1.is_empty()); // No other entities yet

        // Vehicle 2 very close (same block ~50m)
        let e2 = Event::now("v2", -73.9855, 40.7486);
        let out2 = op.process(&e2);
        assert_eq!(out2.len(), 1);
        assert_eq!(out2[0].payload["alert"], "proximity");
        assert_eq!(out2[0].payload["entity_a"], "v2");
        assert_eq!(out2[0].payload["entity_b"], "v1");
    }

    #[test]
    fn test_no_alert_when_far() {
        let mut op = ProximityOperator::new("proximity", 100.0); // 100m threshold

        let e1 = Event::now("v1", -73.9857, 40.7484); // NYC
        op.process(&e1);

        // Vehicle 2 in London — very far
        let e2 = Event::now("v2", -0.1278, 51.5074);
        let out2 = op.process(&e2);
        assert!(out2.is_empty());
    }

    #[test]
    fn test_self_not_triggered() {
        let mut op = ProximityOperator::new("proximity", 1000.0);

        let e1 = Event::now("v1", 0.0, 0.0);
        op.process(&e1);

        // Same entity updates position — should not trigger self-alert
        let e2 = Event::now("v1", 0.0001, 0.0001);
        let out = op.process(&e2);
        assert!(out.is_empty());
    }
}
