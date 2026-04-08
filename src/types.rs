use {
    serde::{Deserialize, Serialize},
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::time::{Instant, SystemTime},
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
    pub system_time: SystemTime,
}

#[derive(Debug, Clone)]
pub struct TransactionUpdate {
    pub slot: u64,
    pub signature: Signature,
    pub instant: Instant,
    pub system_time: SystemTime,
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
    pub transaction_updates: Vec<TransactionUpdate>,
    pub endpoint: String,
}

impl EndpointData {
    pub fn new(endpoint: String, slot_count: usize, buffer_percent: f32) -> Self {
        let buffered_slot_count = (slot_count as f32 * (1.0 + buffer_percent)) as usize;
        let update_capacity = buffered_slot_count * 6;

        Self {
            updates: Vec::with_capacity(update_capacity),
            account_updates: Vec::new(),
            transaction_updates: Vec::new(),
            endpoint,
        }
    }
}
