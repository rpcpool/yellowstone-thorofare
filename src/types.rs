use {
    serde::{Deserialize, Serialize},
    std::time::{Instant, SystemTime},
};

const FINALIZATION_BUFFER_SIZE: usize = 5;

#[derive(Debug, Clone)]
pub struct SlotUpdate {
    pub slot: u64,
    pub status: SlotStatus,
    pub instant: Instant,      // For delta calculations
    pub system_time: SystemTime, // For client visualization
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotStatus {
    FirstShredReceived,
    Completed,
    CreatedBank,
    Processed,
    Confirmed,
    Finalized,
    Dead,
}

impl From<i32> for SlotStatus {
    fn from(value: i32) -> Self {
        match value {
            0 => Self::Processed,
            1 => Self::Confirmed,
            2 => Self::Finalized,
            3 => Self::FirstShredReceived,
            4 => Self::Completed,
            5 => Self::CreatedBank,
            6 => Self::Dead,
            _ => Self::Dead,
        }
    }
}

pub struct EndpointData {
    pub updates: Vec<SlotUpdate>,
    pub endpoint: String,
}

impl EndpointData {
    pub fn new(endpoint: String, slot_count: usize, buffer_percent: f32) -> Self {
        // 6 statuses possible (excluding dead) per slot
        let capacity = ((slot_count as f32 * (1.0 + buffer_percent)) as usize) * 6;

        // Add ~30 slots worth for finalization lag
        let finalization_buffer = 30 * FINALIZATION_BUFFER_SIZE;

        Self {
            updates: Vec::with_capacity(capacity + finalization_buffer),
            endpoint,
        }
    }
}
