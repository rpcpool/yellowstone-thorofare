use {
    serde::{Deserialize, Serialize},
    std::time::{Instant, SystemTime},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
};

#[derive(Debug, Clone)]
pub struct SlotUpdate {
    pub slot: u64,
    pub status: SlotStatus,
    pub instant: Instant,        // For delta calculations
    pub system_time: SystemTime, // For client visualization
}

#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub slot: u64,
    pub pubkey: Pubkey,
    pub write_version: u64,
    pub tx_signature: Signature,
    pub instant: Instant,
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
    pub account_updates: Vec<AccountUpdate>,
    pub endpoint: String,
}

impl EndpointData {
    pub fn new(endpoint: String, slot_count: usize, buffer_percent: f32) -> Self {
        let capacity = Self::calculate_capacity(slot_count, buffer_percent);
        let account_capacity = capacity * 500_000;
        
        Self {
            updates: Vec::with_capacity(capacity),
            account_updates: Vec::with_capacity(account_capacity),
            endpoint,
        }
    }

    pub fn calculate_capacity(slot_count: usize, buffer_percent: f32) -> usize {
        // 6 statuses possible (excluding dead) per slot
        ((slot_count as f32 * (1.0 + buffer_percent)) as usize) * 6
    }
}