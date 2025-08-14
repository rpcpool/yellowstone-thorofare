use {
    crate::types::{AccountUpdate, BlockUpdate, EndpointData, EntryUpdate, SlotStatus, SlotUpdate},
    serde::Serialize,
    std::{
        collections::{HashMap, HashSet},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

#[derive(Debug, Serialize)]
pub struct BenchmarkResult {
    pub version: String,           // Yellowstone Thorofare version
    pub with_accounts: bool,          // Whether --with-accounts was used
    pub grpc_config: GrpcConfigSummary, // gRPC config settings
    pub metadata: Metadata,
    pub endpoints: [EndpointInfo; 2],
    pub endpoint1_summary: EndpointSummary,
    pub endpoint2_summary: EndpointSummary,
    pub slots: Vec<SlotComparison>,
    pub entry_analysis: Option<Vec<EntryAnalysis>>, // Entry-level analysis
    pub entry_timing: Option<Vec<EntryTimingAnalysis>>, // Entry arrival timing
}

#[derive(Debug, Serialize)]
pub struct GrpcConfigSummary {
    pub connect_timeout_ms: u64,
    pub request_timeout_ms: u64,
    pub max_message_size: usize,
    pub use_tls: bool,
    pub http2_adaptive_window: bool,
    pub initial_connection_window_size: Option<u32>,
    pub initial_stream_window_size: Option<u32>,
}

#[derive(Debug, Serialize)]
pub struct Metadata {
    pub total_slots_collected: u64,
    pub common_slots: u64,
    pub compared_slots: u64,
    pub dropped_slots: u64,
    pub duration_ms: u64,
    pub benchmark_start_time: u64,
    pub total_account_updates: Option<(u64, u64)>, // (endpoint1, endpoint2)
    pub total_entries: Option<(u64, u64)>, // (endpoint1, endpoint2)
}

#[derive(Debug, Serialize)]
pub struct EndpointInfo {
    pub endpoint: String,
    pub plugin_type: String,    // "Yellowstone" or "Richat"
    pub plugin_version: String,
    pub avg_ping_ms: f64,
    pub total_updates: u64,
    pub unique_slots: u64,
    pub account_updates: Option<u64>,
    pub entry_updates: Option<u64>,
    pub slots_with_incomplete_updates: Option<u64>, // Slots where we didn't receive all expected account updates
}

#[derive(Debug, Serialize)]
pub struct EndpointSummary {
    pub first_shred_delay: Percentiles,
    pub processing_delay: Percentiles,
    pub confirmation_delay: Percentiles,
    pub finalization_delay: Percentiles,
    pub download_time: Percentiles,
    pub replay_time: Percentiles,
    pub confirmation_time: Percentiles,
    pub finalization_time: Percentiles,
}

#[derive(Debug, Serialize)]
pub struct Percentiles {
    pub p50: f64,
    pub p90: f64,
    pub p99: f64,
}

#[derive(Debug, Serialize)]
pub struct SlotComparison {
    pub slot: u64,
    pub endpoint1: SlotDetail,
    pub endpoint2: SlotDetail,
}

#[derive(Debug, Serialize)]
pub struct SlotDetail {
    pub first_shred_delay_ms: Option<f64>,
    pub processing_delay_ms: Option<f64>,
    pub confirmation_delay_ms: Option<f64>,
    pub finalization_delay_ms: Option<f64>,
    pub transitions: Vec<Transition>,
    pub durations: StageDurations,
    pub account_updates: Vec<AccountUpdateDetail>,
}

#[derive(Debug, Serialize)]
pub struct Transition {
    pub status: String,
    pub timestamp: u64,
}

#[derive(Debug, Serialize)]
pub struct StageDurations {
    pub download_ms: f64,
    pub replay_ms: f64,
    pub confirmation_ms: f64,
    pub finalization_ms: f64,
}

#[derive(Debug, Serialize)]
pub struct AccountUpdateDetail {
    pub pubkey: String,
    pub write_version: u64,
    pub tx_signature: String,
    pub delay_ms: Option<f64>,  // Delay vs other endpoint if matched
    pub timestamp: u64,         // Timestamp in milliseconds since epoch
    pub entry_index: Option<u64>,  // Entry index within the slot where this update originated
}

#[derive(Debug, Serialize)]
pub struct EntryAnalysis {
    pub slot: u64,
    pub entry_index: u64,
    pub endpoint1_account_updates: u64,
    pub endpoint2_account_updates: u64,
    pub endpoint1_succeeded_transactions: u64,
    pub endpoint2_succeeded_transactions: u64,
    pub endpoint1_failed_transactions: u64,
    pub endpoint2_failed_transactions: u64,
    pub endpoint1_total_transactions: u64,
    pub endpoint2_total_transactions: u64,
}

#[derive(Debug, Serialize)]
pub struct EntryTimingAnalysis {
    pub slot: u64,
    pub entry_index: u64,
    pub endpoint1_timestamp: Option<u64>,
    pub endpoint2_timestamp: Option<u64>,
    pub delay_ms: f64,  // How much later one endpoint received it
    pub gap_from_previous_ms: Option<f64>, // Time since previous entry (for cliff detection)
    pub is_cliff: bool, // True if gap > threshold (indicates waiting for missing entries)
}

#[derive(Debug, Default)]
struct EntryStats {
    pub account_updates: u64,
    pub succeeded_transactions: u64,
    pub failed_transactions: u64,
    pub total_transactions: u64,
}

type SlotKey = (u64, SlotStatus);
type AccountKey = (u64, String, String); // (slot, pubkey, signature)

pub struct EndpointMetadata {
    pub plugin_type: String,
    pub plugin_version: String,
}

pub struct Processor;

impl Processor {
    const ENTRY_CLIFF_THRESHOLD_MS: f64 = 50.0; // Gaps > 50ms indicate waiting for missing entries

    pub fn process(
        version: String,
        with_accounts: bool,
        grpc_config: GrpcConfigSummary,
        data1: EndpointData,
        data2: EndpointData,
        meta1: EndpointMetadata,
        meta2: EndpointMetadata,
        ping1: Duration,
        ping2: Duration,
        start_time: Instant,
    ) -> BenchmarkResult {
        let duration_ms = start_time.elapsed().as_millis() as u64;
        let benchmark_start = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - duration_ms;

        let mut endpoint1_incomplete_slots = 0u64;
        let mut endpoint2_incomplete_slots = 0u64;

        // Get unique slots from each endpoint
        let endpoint1_updates = data1.updates.len() as u64;
        let endpoint2_updates = data2.updates.len() as u64;
        
        // Account updates counts
        let endpoint1_account_updates = data1.account_updates.len() as u64;
        let endpoint2_account_updates = data2.account_updates.len() as u64;
        
        // Entry counts
        let endpoint1_entries = data1.entry_updates.len() as u64;
        let endpoint2_entries = data2.entry_updates.len() as u64;

        // Build lookup maps
        let map1 = Self::build_map(data1.updates);
        let map2 = Self::build_map(data2.updates);

        // Analyze entry timing if we have entry data
        let entry_timing = if !data1.entry_updates.is_empty() || !data2.entry_updates.is_empty() {
            let timing = Self::analyze_entry_timing(&data1.entry_updates, &data2.entry_updates);
            
            // Report cliffs
            let cliffs = Self::find_entry_cliffs(&timing);
            if !cliffs.is_empty() {
                eprintln!("Entry processing cliffs detected (gaps > {}ms):", Self::ENTRY_CLIFF_THRESHOLD_MS);
                for (slot, entry_idx, endpoint, gap) in cliffs {
                    eprintln!("  Slot {} Entry {} (Endpoint {}): {}ms gap", slot, entry_idx, endpoint, gap);
                }
            }
            
            Some(timing)
        } else {
            None
        };

        // Build expected account counts from blocks
        let expected_updates1 = Self::get_expected_account_updates(&data1.block_updates);
        let expected_updates2 = Self::get_expected_account_updates(&data2.block_updates);
        
        // Build tx to entry mappings if we have block data
        let (tx_to_entry_map1, entry_stats1) = if with_accounts && !data1.block_updates.is_empty() {
            Self::build_tx_to_entry_map_with_stats(&data1.block_updates, &data1.account_updates)
        } else {
            (HashMap::new(), HashMap::new())
        };
        
        let (tx_to_entry_map2, entry_stats2) = if with_accounts && !data2.block_updates.is_empty() {
            Self::build_tx_to_entry_map_with_stats(&data2.block_updates, &data2.account_updates)
        } else {
            (HashMap::new(), HashMap::new())
        };

        // Build account maps if we have account data
        let (accounts1_by_slot, accounts2_by_slot, account_match_map) = if with_accounts {
            let accounts1 = Self::group_accounts_by_slot(&data1.account_updates);
            let accounts2 = Self::group_accounts_by_slot(&data2.account_updates);
            let matches = Self::build_account_match_map(&data1.account_updates, &data2.account_updates);
            (accounts1, accounts2, matches)
        } else {
            (HashMap::new(), HashMap::new(), HashMap::new())
        };

        // Count unique slots per endpoint
        let endpoint1_slots: HashSet<u64> = map1.keys().map(|(slot, _)| *slot).collect();
        let endpoint2_slots: HashSet<u64> = map2.keys().map(|(slot, _)| *slot).collect();

        let total_slots_collected = endpoint1_slots.len() as u64 + endpoint2_slots.len() as u64;

        // Find slots that exist in both endpoints
        let common_slots: Vec<u64> = endpoint1_slots
            .intersection(&endpoint2_slots)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .collect::<HashSet<_>>() // dedup
            .into_iter()
            .collect::<Vec<_>>();

        let mut sorted_common_slots = common_slots;
        sorted_common_slots.sort_unstable();

        let common_slots_count = sorted_common_slots.len() as u64;

        // Required statuses for a complete slot
        let required_statuses = [
            SlotStatus::FirstShredReceived,
            SlotStatus::Completed,
            SlotStatus::CreatedBank,
            SlotStatus::Processed,
            SlotStatus::Confirmed,
            SlotStatus::Finalized,
        ];

        // Process slots
        let mut slot_comparisons = Vec::new();
        let mut compared_slots = 0u64;
        let mut dropped_slots = 0u64;

        let mut endpoint1_first_shred_delays = Vec::new();
        let mut endpoint1_processing_delays = Vec::new();
        let mut endpoint1_confirmation_delays = Vec::new();
        let mut endpoint1_finalization_delays = Vec::new();
        let mut endpoint1_download_times = Vec::new();
        let mut endpoint1_replay_times = Vec::new();
        let mut endpoint1_confirmation_times = Vec::new();
        let mut endpoint1_finalization_times = Vec::new();

        let mut endpoint2_first_shred_delays = Vec::new();
        let mut endpoint2_processing_delays = Vec::new();
        let mut endpoint2_confirmation_delays = Vec::new();
        let mut endpoint2_finalization_delays = Vec::new();
        let mut endpoint2_download_times = Vec::new();
        let mut endpoint2_replay_times = Vec::new();
        let mut endpoint2_confirmation_times = Vec::new();
        let mut endpoint2_finalization_times = Vec::new();

        for slot in sorted_common_slots {
            // Check if slot is dead in either endpoint
            if map1.contains_key(&(slot, SlotStatus::Dead))
                || map2.contains_key(&(slot, SlotStatus::Dead))
            {
                dropped_slots += 1;
                continue;
            }

            // Check if both endpoints have all required statuses
            let ep1_complete = required_statuses
                .iter()
                .all(|status| map1.contains_key(&(slot, *status)));
            let ep2_complete = required_statuses
                .iter()
                .all(|status| map2.contains_key(&(slot, *status)));

            if !ep1_complete || !ep2_complete {
                dropped_slots += 1;
                continue;
            }

            // Check if we have all expected account updates using block's updated_account_count
            if with_accounts {
                let received1 = accounts1_by_slot.get(&slot).map(|v| v.len() as u64).unwrap_or(0);
                let received2 = accounts2_by_slot.get(&slot).map(|v| v.len() as u64).unwrap_or(0);
                let expected1 = expected_updates1.get(&slot).copied().unwrap_or(0);
                let expected2 = expected_updates2.get(&slot).copied().unwrap_or(0);
                
                // Track incomplete slots
                if expected1 > 0 && received1 < expected1 {
                    endpoint1_incomplete_slots += 1;
                }
                if expected2 > 0 && received2 < expected2 {
                    endpoint2_incomplete_slots += 1;
                }
                
                // Skip slots where either endpoint doesn't have all expected updates
                if (expected1 > 0 && received1 < expected1) || (expected2 > 0 && received2 < expected2) {
                    eprintln!(
                        "Slot {}: Incomplete account updates. Endpoint1: {}/{}, Endpoint2: {}/{}",
                        slot, received1, expected1, received2, expected2
                    );
                    dropped_slots += 1;
                    continue;
                }
                
                // Also check for mismatched write versions (same account updated different times)
                let mut has_mismatch = false;
                for ((s, _pk, _sig), (vec1, vec2)) in &account_match_map {
                    if *s == slot && vec1.len() != vec2.len() {
                        has_mismatch = true;
                        break;
                    }
                }
                
                if has_mismatch {
                    dropped_slots += 1;
                    continue;
                }
            }

            // Calculate first shred delays
            let (ep1_first_shred, ep2_first_shred) = Self::calc_first_shred_delays(&map1, &map2, slot);

            // Collect first shred delays
            if let Some(delay) = ep1_first_shred {
                endpoint1_first_shred_delays.push(delay);
            }
            if let Some(delay) = ep2_first_shred {
                endpoint2_first_shred_delays.push(delay);
            }

            // Calculate processing delays
            let (ep1_processing_delay, ep2_processing_delay) = Self::calc_processing_delays(&map1, &map2, slot);

            // Collect processing delays
            if let Some(delay) = ep1_processing_delay {
                endpoint1_processing_delays.push(delay);
            }
            if let Some(delay) = ep2_processing_delay {
                endpoint2_processing_delays.push(delay);
            }

            // Calculate confirmation delays
            let (ep1_confirmation_delay, ep2_confirmation_delay) = Self::calc_confirmation_delays(&map1, &map2, slot);

            // Collect confirmation delays
            if let Some(delay) = ep1_confirmation_delay {
                endpoint1_confirmation_delays.push(delay);
            }
            if let Some(delay) = ep2_confirmation_delay {
                endpoint2_confirmation_delays.push(delay);
            }

            // Calculate finalization delays
            let (ep1_finalization_delay, ep2_finalization_delay) = Self::calc_finalization_delays(&map1, &map2, slot);

            // Collect finalization delays
            if let Some(delay) = ep1_finalization_delay {
                endpoint1_finalization_delays.push(delay);
            }
            if let Some(delay) = ep2_finalization_delay {
                endpoint2_finalization_delays.push(delay);
            }

            // Build endpoint-specific data
            let mut endpoint1_detail = Self::build_slot_detail(&map1, slot);
            let mut endpoint2_detail = Self::build_slot_detail(&map2, slot);

            // Add first shred delays to slot details
            endpoint1_detail.first_shred_delay_ms = ep1_first_shred.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.first_shred_delay_ms = ep2_first_shred.map(|d| d.as_secs_f64() * 1000.0);

            // Add processing delays to slot details
            endpoint1_detail.processing_delay_ms = ep1_processing_delay.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.processing_delay_ms = ep2_processing_delay.map(|d| d.as_secs_f64() * 1000.0);

            // Add confirmation delays to slot details
            endpoint1_detail.confirmation_delay_ms = ep1_confirmation_delay.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.confirmation_delay_ms = ep2_confirmation_delay.map(|d| d.as_secs_f64() * 1000.0);

            // Add finalization delays to slot details
            endpoint1_detail.finalization_delay_ms = ep1_finalization_delay.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.finalization_delay_ms = ep2_finalization_delay.map(|d| d.as_secs_f64() * 1000.0);

            // Add account updates to slot details
            if with_accounts {
                endpoint1_detail.account_updates = Self::build_account_details(
                    accounts1_by_slot.get(&slot),
                    &account_match_map,
                    1,  // endpoint 1
                    &tx_to_entry_map1,
                );
                endpoint2_detail.account_updates = Self::build_account_details(
                    accounts2_by_slot.get(&slot),
                    &account_match_map,
                    2,  // endpoint 2
                    &tx_to_entry_map2,
                );
            }

            // Collect metrics (we know these exist due to the check above)
            let d1 = Self::calc_download(&map1, slot).unwrap();
            let d2 = Self::calc_download(&map2, slot).unwrap();
            endpoint1_download_times.push(d1);
            endpoint2_download_times.push(d2);

            let r1 = Self::calc_replay(&map1, slot).unwrap();
            let r2 = Self::calc_replay(&map2, slot).unwrap();
            endpoint1_replay_times.push(r1);
            endpoint2_replay_times.push(r2);

            let c1 = Self::calc_confirmation(&map1, slot).unwrap();
            let c2 = Self::calc_confirmation(&map2, slot).unwrap();
            endpoint1_confirmation_times.push(c1);
            endpoint2_confirmation_times.push(c2);

            let f1 = Self::calc_finalization(&map1, slot).unwrap();
            let f2 = Self::calc_finalization(&map2, slot).unwrap();
            endpoint1_finalization_times.push(f1);
            endpoint2_finalization_times.push(f2);

            slot_comparisons.push(SlotComparison {
                slot,
                endpoint1: endpoint1_detail,
                endpoint2: endpoint2_detail,
            });

            compared_slots += 1;
        }

        // Build entry analysis if we have the data
        let entry_analysis = if with_accounts && !entry_stats1.is_empty() && !entry_stats2.is_empty() {
            Some(Self::build_entry_analysis(&entry_stats1, &entry_stats2))
        } else {
            None
        };

        BenchmarkResult {
            version,
            with_accounts,
            grpc_config,
            metadata: Metadata {
                total_slots_collected,
                common_slots: common_slots_count,
                compared_slots,
                dropped_slots,
                duration_ms,
                benchmark_start_time: benchmark_start,
                total_account_updates: if with_accounts {
                    Some((endpoint1_account_updates, endpoint2_account_updates))
                } else {
                    None
                },
                total_entries: if endpoint1_entries > 0 || endpoint2_entries > 0 {
                    Some((endpoint1_entries, endpoint2_entries))
                } else {
                    None
                },
            },
            endpoints: [
                EndpointInfo {
                    endpoint: data1.endpoint,
                    plugin_type: meta1.plugin_type,
                    plugin_version: meta1.plugin_version,
                    avg_ping_ms: ping1.as_secs_f64() * 1000.0,
                    total_updates: endpoint1_updates,
                    unique_slots: endpoint1_slots.len() as u64,
                    account_updates: if with_accounts { Some(endpoint1_account_updates) } else { None },
                    entry_updates: if endpoint1_entries > 0 { Some(endpoint1_entries) } else { None },
                    slots_with_incomplete_updates: if with_accounts { Some(endpoint1_incomplete_slots) } else { None },
                },
                EndpointInfo {
                    endpoint: data2.endpoint,
                    plugin_type: meta2.plugin_type,
                    plugin_version: meta2.plugin_version,
                    avg_ping_ms: ping2.as_secs_f64() * 1000.0,
                    total_updates: endpoint2_updates,
                    unique_slots: endpoint2_slots.len() as u64,
                    account_updates: if with_accounts { Some(endpoint2_account_updates) } else { None },
                    entry_updates: if endpoint2_entries > 0 { Some(endpoint2_entries) } else { None },
                    slots_with_incomplete_updates: if with_accounts { Some(endpoint2_incomplete_slots) } else { None },
                },
            ],
            endpoint1_summary: EndpointSummary {
                first_shred_delay: Self::percentiles(endpoint1_first_shred_delays),
                processing_delay: Self::percentiles(endpoint1_processing_delays),
                confirmation_delay: Self::percentiles(endpoint1_confirmation_delays),
                finalization_delay: Self::percentiles(endpoint1_finalization_delays),
                download_time: Self::percentiles(endpoint1_download_times),
                replay_time: Self::percentiles(endpoint1_replay_times),
                confirmation_time: Self::percentiles(endpoint1_confirmation_times),
                finalization_time: Self::percentiles(endpoint1_finalization_times),
            },
            endpoint2_summary: EndpointSummary {
                first_shred_delay: Self::percentiles(endpoint2_first_shred_delays),
                processing_delay: Self::percentiles(endpoint2_processing_delays),
                confirmation_delay: Self::percentiles(endpoint2_confirmation_delays),
                finalization_delay: Self::percentiles(endpoint2_finalization_delays),
                download_time: Self::percentiles(endpoint2_download_times),
                replay_time: Self::percentiles(endpoint2_replay_times),
                confirmation_time: Self::percentiles(endpoint2_confirmation_times),
                finalization_time: Self::percentiles(endpoint2_finalization_times),
            },
            slots: slot_comparisons,
            entry_analysis,
            entry_timing,
        }
    }

    // Analyze entry timing between endpoints
    fn analyze_entry_timing(
        entries1: &[EntryUpdate],
        entries2: &[EntryUpdate],
    ) -> Vec<EntryTimingAnalysis> {
        // Group entries by (slot, index)
        let mut entries1_map: HashMap<(u64, u64), EntryUpdate> = HashMap::new();
        let mut entries2_map: HashMap<(u64, u64), EntryUpdate> = HashMap::new();
        
        for entry in entries1 {
            entries1_map.insert((entry.slot, entry.index), entry.clone());
        }
        
        for entry in entries2 {
            entries2_map.insert((entry.slot, entry.index), entry.clone());
        }
        
        // Find all unique (slot, index) pairs
        let mut all_keys: HashSet<(u64, u64)> = HashSet::new();
        all_keys.extend(entries1_map.keys());
        all_keys.extend(entries2_map.keys());
        
        let mut analysis: Vec<EntryTimingAnalysis> = Vec::new();
        
        for (slot, index) in all_keys {
            let e1 = entries1_map.get(&(slot, index));
            let e2 = entries2_map.get(&(slot, index));
            
            let (ts1, ts2, delay_ms) = match (e1, e2) {
                (Some(e1), Some(e2)) => {
                    let ts1 = Self::to_timestamp_ms(e1.system_time);
                    let ts2 = Self::to_timestamp_ms(e2.system_time);
                    let delay = if ts1 < ts2 {
                        (ts2 - ts1) as f64
                    } else {
                        (ts1 - ts2) as f64
                    };
                    (Some(ts1), Some(ts2), delay)
                }
                (Some(e1), None) => (Some(Self::to_timestamp_ms(e1.system_time)), None, 0.0),
                (None, Some(e2)) => (None, Some(Self::to_timestamp_ms(e2.system_time)), 0.0),
                _ => (None, None, 0.0),
            };
            
            analysis.push(EntryTimingAnalysis {
                slot,
                entry_index: index,
                endpoint1_timestamp: ts1,
                endpoint2_timestamp: ts2,
                delay_ms,
                gap_from_previous_ms: None,
                is_cliff: false,
            });
        }
        
        // Sort by slot and entry index
        analysis.sort_by_key(|a| (a.slot, a.entry_index));
        
        // Calculate gaps from previous entry and detect cliffs
        for i in 1..analysis.len() {
            if analysis[i].slot == analysis[i-1].slot {
                // Same slot, calculate gaps for each endpoint
                if let (Some(curr_ts1), Some(prev_ts1)) = (analysis[i].endpoint1_timestamp, analysis[i-1].endpoint1_timestamp) {
                    let gap = (curr_ts1 - prev_ts1) as f64;
                    analysis[i].gap_from_previous_ms = Some(gap);
                    analysis[i].is_cliff = gap > Self::ENTRY_CLIFF_THRESHOLD_MS;
                } else if let (Some(curr_ts2), Some(prev_ts2)) = (analysis[i].endpoint2_timestamp, analysis[i-1].endpoint2_timestamp) {
                    let gap = (curr_ts2 - prev_ts2) as f64;
                    analysis[i].gap_from_previous_ms = Some(gap);
                    analysis[i].is_cliff = gap > Self::ENTRY_CLIFF_THRESHOLD_MS;
                }
            }
        }
        
        analysis
    }

    // Find entry processing "cliffs" where delays spike
    fn find_entry_cliffs(analysis: &Vec<EntryTimingAnalysis>) -> Vec<(u64, u64, u8, f64)> {
        let mut cliffs = Vec::new();
        
        for entry in analysis {
            if entry.is_cliff {
                if let Some(gap) = entry.gap_from_previous_ms {
                    // Determine which endpoint had the cliff
                    let endpoint = if entry.endpoint1_timestamp.is_some() { 1 } else { 2 };
                    cliffs.push((entry.slot, entry.entry_index, endpoint, gap));
                }
            }
        }
        
        cliffs
    }

    // Get expected account update counts from blocks
    fn get_expected_account_updates(blocks: &[BlockUpdate]) -> HashMap<u64, u64> {
        blocks
            .iter()
            .map(|block| (block.slot, block.updated_account_count))
            .collect()
    }

    // Build mapping from transaction signature to entry index with statistics
    fn build_tx_to_entry_map_with_stats(
        block_updates: &[BlockUpdate],
        account_updates: &[AccountUpdate],
    ) -> (HashMap<(u64, String), u64>, HashMap<(u64, u64), EntryStats>) {
        let mut tx_to_entry = HashMap::new();
        let mut entry_stats: HashMap<(u64, u64), EntryStats> = HashMap::new();
        
        // First, build a set of successful transaction signatures from account updates
        let successful_txs: HashSet<(u64, String)> = account_updates
            .iter()
            .map(|update| (update.slot, update.tx_signature.to_string()))
            .collect();
        
        for block in block_updates {
            let slot = block.slot;
            
            // Sort entries by index to process in order
            let mut entries = block.entries.clone();
            entries.sort_by_key(|e| e.index);
            
            // Verify entries are contiguous and handle gaps
            let mut expected_index = 0u64;
            for entry in &entries {
                if entry.index != expected_index {
                    eprintln!(
                        "Warning: Non-contiguous entries in slot {}: expected index {}, got {}",
                        slot, expected_index, entry.index
                    );
                }
                expected_index = entry.index + 1;
            }
            
            for (i, entry) in entries.iter().enumerate() {
                let entry_start = entry.starting_transaction_index;
                
                // Find where this entry ends
                let entry_end = if i + 1 < entries.len() {
                    entries[i + 1].starting_transaction_index
                } else {
                    block.transactions.len() as u64
                };
                
                // Validate the range
                if entry_end < entry_start {
                    eprintln!(
                        "Warning: Invalid entry range in slot {}, entry {}: start={}, end={}",
                        slot, entry.index, entry_start, entry_end
                    );
                    continue;
                }
                
                let mut stats = EntryStats::default();
                stats.total_transactions = entry_end - entry_start;
                
                // Map all transactions in this range to this entry
                for tx_idx in entry_start..entry_end {
                    if let Some(tx) = block.transactions.get(tx_idx as usize) {
                        let tx_key = (slot, tx.signature.to_string());
                        
                        // Check for duplicate transaction signatures in different entries
                        if let Some(&existing_entry) = tx_to_entry.get(&tx_key) {
                            if existing_entry != entry.index {
                                eprintln!(
                                    "Warning: Transaction {} appears in multiple entries: {} and {}",
                                    tx.signature, existing_entry, entry.index
                                );
                            }
                        }
                        
                        tx_to_entry.insert(tx_key.clone(), entry.index);
                        
                        // Check if transaction was successful (has account updates)
                        if successful_txs.contains(&tx_key) {
                            stats.succeeded_transactions += 1;
                        } else {
                            stats.failed_transactions += 1;
                        }
                    }
                }
                
                entry_stats.insert((slot, entry.index), stats);
            }
        }
        
        // Count account updates per entry
        for update in account_updates {
            let tx_key = (update.slot, update.tx_signature.to_string());
            if let Some(&entry_index) = tx_to_entry.get(&tx_key) {
                if let Some(stats) = entry_stats.get_mut(&(update.slot, entry_index)) {
                    stats.account_updates += 1;
                }
            }
        }
        
        (tx_to_entry, entry_stats)
    }

    // Build entry analysis comparing both endpoints
    fn build_entry_analysis(
        stats1: &HashMap<(u64, u64), EntryStats>,
        stats2: &HashMap<(u64, u64), EntryStats>,
    ) -> Vec<EntryAnalysis> {
        let mut all_keys: HashSet<(u64, u64)> = HashSet::new();
        all_keys.extend(stats1.keys());
        all_keys.extend(stats2.keys());
        
        let mut analysis: Vec<EntryAnalysis> = all_keys
            .into_iter()
            .map(|(slot, entry_index)| {
                let s1 = stats1.get(&(slot, entry_index));
                let s2 = stats2.get(&(slot, entry_index));
                
                EntryAnalysis {
                    slot,
                    entry_index,
                    endpoint1_account_updates: s1.map_or(0, |s| s.account_updates),
                    endpoint2_account_updates: s2.map_or(0, |s| s.account_updates),
                    endpoint1_succeeded_transactions: s1.map_or(0, |s| s.succeeded_transactions),
                    endpoint2_succeeded_transactions: s2.map_or(0, |s| s.succeeded_transactions),
                    endpoint1_failed_transactions: s1.map_or(0, |s| s.failed_transactions),
                    endpoint2_failed_transactions: s2.map_or(0, |s| s.failed_transactions),
                    endpoint1_total_transactions: s1.map_or(0, |s| s.total_transactions),
                    endpoint2_total_transactions: s2.map_or(0, |s| s.total_transactions),
                }
            })
            .collect();
        
        // Sort by slot and entry index
        analysis.sort_by_key(|a| (a.slot, a.entry_index));
        
        analysis
    }

    // Group account updates by slot
    fn group_accounts_by_slot(updates: &[AccountUpdate]) -> HashMap<u64, Vec<AccountUpdate>> {
        let mut by_slot = HashMap::new();
        for update in updates {
            by_slot
                .entry(update.slot)
                .or_insert_with(|| Vec::with_capacity(100))
                .push(update.clone());
        }
        by_slot
    }

    // Build map to match account updates between endpoints
    fn build_account_match_map(
        updates1: &[AccountUpdate],
        updates2: &[AccountUpdate],
    ) -> HashMap<AccountKey, (Vec<(u64, Instant)>, Vec<(u64, Instant)>)> {
        let mut map = HashMap::new();
        
        // Group by (slot, pubkey, signature) and keep all write_versions
        for update in updates1 {
            let key = (
                update.slot,
                update.pubkey.to_string(),
                update.tx_signature.to_string(),
            );
            map.entry(key)
                .or_insert((Vec::new(), Vec::new()))
                .0.push((update.write_version, update.instant));
        }
        
        for update in updates2 {
            let key = (
                update.slot,
                update.pubkey.to_string(),
                update.tx_signature.to_string(),
            );
            map.entry(key)
                .or_insert((Vec::new(), Vec::new()))
                .1.push((update.write_version, update.instant));
        }
        
        // Sort each vec by write_version to match first-to-first, second-to-second
        for (_, (vec1, vec2)) in map.iter_mut() {
            vec1.sort_by_key(|(wv, _)| *wv);
            vec2.sort_by_key(|(wv, _)| *wv);
        }
        
        map
    }

    // Build account details for JSON output
    fn build_account_details(
        updates: Option<&Vec<AccountUpdate>>,
        match_map: &HashMap<AccountKey, (Vec<(u64, Instant)>, Vec<(u64, Instant)>)>,
        endpoint_num: usize,
        tx_to_entry_map: &HashMap<(u64, String), u64>,
    ) -> Vec<AccountUpdateDetail> {
        let mut details = Vec::new();
        
        if let Some(updates) = updates {
            for update in updates {
                let key = (
                    update.slot,
                    update.pubkey.to_string(),
                    update.tx_signature.to_string(),
                );
                
                // Calculate delay if matched
                let delay_ms = if let Some((vec1, vec2)) = match_map.get(&key) {
                    match (vec1.first(), vec2.first()) {
                        (Some((_, instant1)), Some((_, instant2))) => {
                            if endpoint_num == 1 {
                                if instant1 > instant2 {
                                    Some(instant1.duration_since(*instant2).as_secs_f64() * 1000.0)
                                } else {
                                    Some(0.0)
                                }
                            } else {
                                if instant2 > instant1 {
                                    Some(instant2.duration_since(*instant1).as_secs_f64() * 1000.0)
                                } else {
                                    Some(0.0)
                                }
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                
                // Look up entry index from tx_signature
                let entry_index = tx_to_entry_map.get(&(update.slot, update.tx_signature.to_string())).copied();
                
                details.push(AccountUpdateDetail {
                    pubkey: update.pubkey.to_string(),
                    write_version: update.write_version,
                    tx_signature: update.tx_signature.to_string(),
                    timestamp: Self::to_timestamp_ms(update.system_time),
                    delay_ms,
                    entry_index,
                });
            }
        }
        
        details
    }

    fn calc_delays(
        map1: &HashMap<SlotKey, SlotUpdate>,
        map2: &HashMap<SlotKey, SlotUpdate>,
        slot: u64,
        slot_status: SlotStatus,
    ) -> (Option<Duration>, Option<Duration>) {
        let key = (slot, slot_status);
        
        match (map1.get(&key), map2.get(&key)) {
            (Some(u1), Some(u2)) => {
                if u1.instant < u2.instant {
                    let wait_time = u2.instant.duration_since(u1.instant);
                    (Some(Duration::from_secs(0)), Some(wait_time))
                } else if u2.instant < u1.instant {
                    let wait_time = u1.instant.duration_since(u2.instant);
                    (Some(wait_time), Some(Duration::from_secs(0)))
                } else {
                    (Some(Duration::from_secs(0)), Some(Duration::from_secs(0)))
                }
            }
            _ => (None, None),
        }
    }

    fn calc_first_shred_delays(
        map1: &HashMap<SlotKey, SlotUpdate>,
        map2: &HashMap<SlotKey, SlotUpdate>,
        slot: u64,
    ) -> (Option<Duration>, Option<Duration>) {
       Self::calc_delays(map1, map2, slot, SlotStatus::FirstShredReceived)
    }

    fn calc_processing_delays(
        map1: &HashMap<SlotKey, SlotUpdate>,
        map2: &HashMap<SlotKey, SlotUpdate>,
        slot: u64,
    ) -> (Option<Duration>, Option<Duration>) {
        Self::calc_delays(map1, map2, slot, SlotStatus::Processed)
    }

    fn calc_confirmation_delays(
        map1: &HashMap<SlotKey, SlotUpdate>,
        map2: &HashMap<SlotKey, SlotUpdate>,
        slot: u64,
    ) -> (Option<Duration>, Option<Duration>) {
        Self::calc_delays(map1, map2, slot, SlotStatus::Confirmed)
    }

    fn calc_finalization_delays(
        map1: &HashMap<SlotKey, SlotUpdate>,
        map2: &HashMap<SlotKey, SlotUpdate>,
        slot: u64,
    ) -> (Option<Duration>, Option<Duration>) {
        Self::calc_delays(map1, map2, slot, SlotStatus::Finalized)
    }

    fn build_slot_detail(map: &HashMap<SlotKey, SlotUpdate>, slot: u64) -> SlotDetail {
        let statuses = [
            SlotStatus::FirstShredReceived,
            SlotStatus::Completed,
            SlotStatus::CreatedBank,
            SlotStatus::Processed,
            SlotStatus::Confirmed,
            SlotStatus::Finalized,
        ];

        let mut transitions = Vec::new();
        for status in &statuses {
            if let Some(update) = map.get(&(slot, *status)) {
                transitions.push(Transition {
                    status: format!("{:?}", status),
                    timestamp: Self::to_timestamp_ms(update.system_time),
                });
            }
        }

        let durations = StageDurations {
            download_ms: Self::calc_download(map, slot).unwrap().as_secs_f64() * 1000.0,
            replay_ms: Self::calc_replay(map, slot).unwrap().as_secs_f64() * 1000.0,
            confirmation_ms: Self::calc_confirmation(map, slot).unwrap().as_secs_f64() * 1000.0,
            finalization_ms: Self::calc_finalization(map, slot).unwrap().as_secs_f64() * 1000.0,
        };

        SlotDetail {
            first_shred_delay_ms: None,
            processing_delay_ms: None,
            confirmation_delay_ms: None,
            finalization_delay_ms: None,
            transitions,
            durations,
            account_updates: Vec::new(),
        }
    }

    fn calc_download(map: &HashMap<SlotKey, SlotUpdate>, slot: u64) -> Option<Duration> {
        let first = map.get(&(slot, SlotStatus::FirstShredReceived))?;
        let completed = map.get(&(slot, SlotStatus::Completed))?;
        Some(completed.instant.duration_since(first.instant))
    }

    fn calc_replay(map: &HashMap<SlotKey, SlotUpdate>, slot: u64) -> Option<Duration> {
        let bank = map.get(&(slot, SlotStatus::CreatedBank))?;
        let processed = map.get(&(slot, SlotStatus::Processed))?;
        Some(processed.instant.duration_since(bank.instant))
    }

    fn calc_confirmation(map: &HashMap<SlotKey, SlotUpdate>, slot: u64) -> Option<Duration> {
        let processed = map.get(&(slot, SlotStatus::Processed))?;
        let confirmed = map.get(&(slot, SlotStatus::Confirmed))?;
        Some(confirmed.instant.duration_since(processed.instant))
    }

    fn calc_finalization(map: &HashMap<SlotKey, SlotUpdate>, slot: u64) -> Option<Duration> {
        let confirmed = map.get(&(slot, SlotStatus::Confirmed))?;
        let finalized = map.get(&(slot, SlotStatus::Finalized))?;
        Some(finalized.instant.duration_since(confirmed.instant))
    }

    fn build_map(updates: Vec<SlotUpdate>) -> HashMap<SlotKey, SlotUpdate> {
        updates
            .into_iter()
            .map(|u| ((u.slot, u.status), u))
            .collect()
    }

    fn to_timestamp_ms(time: SystemTime) -> u64 {
        time.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
    }

    fn percentiles(mut values: Vec<Duration>) -> Percentiles {
        if values.is_empty() {
            return Percentiles {
                p50: 0.0,
                p90: 0.0,
                p99: 0.0,
            };
        }

        values.sort_unstable();
        let len = values.len();

        Percentiles {
            p50: values[len.saturating_sub(1).min(len * 50 / 100)].as_secs_f64() * 1000.0,
            p90: values[len.saturating_sub(1).min(len * 90 / 100)].as_secs_f64() * 1000.0,
            p99: values[len.saturating_sub(1).min(len * 99 / 100)].as_secs_f64() * 1000.0,
        }
    }
}