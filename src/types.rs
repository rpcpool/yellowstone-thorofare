use {
    serde::{Deserialize, Serialize},
    std::time::SystemTime,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotUpdate {
    pub slot: u64,
    pub status: SlotStatus,
    pub timestamp: SystemTime,
    pub parent: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
