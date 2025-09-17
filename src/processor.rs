use {
    crate::types::{AccountUpdate, EndpointData, SlotStatus, SlotUpdate},
    serde::Serialize,
    std::{
        collections::{HashMap, HashSet},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

#[derive(Debug, Serialize)]
pub struct BenchmarkResult {
    pub version: String,                // Yellowstone Thorofare version
    pub with_accounts: bool,            // Whether --with-accounts was used
    pub account_owner: Option<String>,  // The account owner pubkey filter (if specified)
    pub grpc_config: GrpcConfigSummary, // gRPC config settings
    pub metadata: Metadata,
    pub endpoints: [EndpointInfo; 2],
    pub endpoint1_summary: EndpointSummary,
    pub endpoint2_summary: EndpointSummary,
    pub slots: Vec<SlotComparison>,
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
}

#[derive(Debug, Serialize)]
pub struct EndpointInfo {
    pub endpoint: String,
    pub plugin_type: String, // "Yellowstone" or "Richat"
    pub plugin_version: String,
    pub avg_ping_ms: f64,
    pub total_updates: u64,
    pub unique_slots: u64,
    pub account_updates: Option<u64>,
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
    pub account_delay: Option<Percentiles>,
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
    pub delay_ms: Option<f64>, // Delay vs other endpoint if matched
    pub timestamp: u64,        // Timestamp in milliseconds since epoch
}

type SlotKey = (u64, SlotStatus);
type AccountKey = (u64, String, String); // (slot, pubkey, signature)

pub struct EndpointMetadata {
    pub plugin_type: String,
    pub plugin_version: String,
}

pub struct Processor;

impl Processor {
    pub fn process(
        version: String,
        with_accounts: bool,
        account_owner: Option<String>,
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

        // Get unique slots from each endpoint
        let endpoint1_updates = data1.updates.len() as u64;
        let endpoint2_updates = data2.updates.len() as u64;

        // Account updates counts
        let endpoint1_account_updates = data1.account_updates.len() as u64;
        let endpoint2_account_updates = data2.account_updates.len() as u64;

        // Build lookup maps
        let map1 = Self::build_map(data1.updates);
        let map2 = Self::build_map(data2.updates);

        // Build account maps if we have account data
        let (accounts1_by_slot, accounts2_by_slot, account_match_map) = if with_accounts {
            let accounts1 = Self::group_accounts_by_slot(&data1.account_updates);
            let accounts2 = Self::group_accounts_by_slot(&data2.account_updates);
            let matches =
                Self::build_account_match_map(&data1.account_updates, &data2.account_updates);
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

        // Account update delay collections
        let mut endpoint1_account_delays = Vec::new();
        let mut endpoint2_account_delays = Vec::new();

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

            // Check if we have the same number of account updates so our benchmark is fair
            if with_accounts {
                let received1 = accounts1_by_slot
                    .get(&slot)
                    .map(|v| v.len() as u64)
                    .unwrap_or(0);
                let received2 = accounts2_by_slot
                    .get(&slot)
                    .map(|v| v.len() as u64)
                    .unwrap_or(0);

                // Skip slots where we don't have the same number of account updates.
                if received1 != received2 {
                    dropped_slots += 1;
                    continue;
                }
            }

            // Calculate first shred delays
            let (ep1_first_shred, ep2_first_shred) =
                Self::calc_first_shred_delays(&map1, &map2, slot);

            // Collect first shred delays
            if let Some(delay) = ep1_first_shred {
                endpoint1_first_shred_delays.push(delay);
            }
            if let Some(delay) = ep2_first_shred {
                endpoint2_first_shred_delays.push(delay);
            }

            // Calculate processing delays
            let (ep1_processing_delay, ep2_processing_delay) =
                Self::calc_processing_delays(&map1, &map2, slot);

            // Collect processing delays
            if let Some(delay) = ep1_processing_delay {
                endpoint1_processing_delays.push(delay);
            }
            if let Some(delay) = ep2_processing_delay {
                endpoint2_processing_delays.push(delay);
            }

            // Calculate confirmation delays
            let (ep1_confirmation_delay, ep2_confirmation_delay) =
                Self::calc_confirmation_delays(&map1, &map2, slot);

            // Collect confirmation delays
            if let Some(delay) = ep1_confirmation_delay {
                endpoint1_confirmation_delays.push(delay);
            }
            if let Some(delay) = ep2_confirmation_delay {
                endpoint2_confirmation_delays.push(delay);
            }

            // Calculate finalization delays
            let (ep1_finalization_delay, ep2_finalization_delay) =
                Self::calc_finalization_delays(&map1, &map2, slot);

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
            endpoint1_detail.first_shred_delay_ms =
                ep1_first_shred.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.first_shred_delay_ms =
                ep2_first_shred.map(|d| d.as_secs_f64() * 1000.0);

            // Add processing delays to slot details
            endpoint1_detail.processing_delay_ms =
                ep1_processing_delay.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.processing_delay_ms =
                ep2_processing_delay.map(|d| d.as_secs_f64() * 1000.0);

            // Add confirmation delays to slot details
            endpoint1_detail.confirmation_delay_ms =
                ep1_confirmation_delay.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.confirmation_delay_ms =
                ep2_confirmation_delay.map(|d| d.as_secs_f64() * 1000.0);

            // Add finalization delays to slot details
            endpoint1_detail.finalization_delay_ms =
                ep1_finalization_delay.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.finalization_delay_ms =
                ep2_finalization_delay.map(|d| d.as_secs_f64() * 1000.0);

            // Add account updates to slot details
            if with_accounts {
                endpoint1_detail.account_updates = Self::build_account_details(
                    accounts1_by_slot.get(&slot),
                    &account_match_map,
                    1, // endpoint 1
                );
                endpoint2_detail.account_updates = Self::build_account_details(
                    accounts2_by_slot.get(&slot),
                    &account_match_map,
                    2, // endpoint 2
                );
                
                // Collect account delays for percentile calculation 
                for update in &endpoint1_detail.account_updates {
                    if let Some(delay_ms) = update.delay_ms {
                        if delay_ms > 0.0 {
                            endpoint1_account_delays.push(Duration::from_secs_f64(delay_ms / 1000.0));
                        }
                    }
                }
                for update in &endpoint2_detail.account_updates {
                    if let Some(delay_ms) = update.delay_ms {
                        if delay_ms > 0.0 {
                            endpoint2_account_delays.push(Duration::from_secs_f64(delay_ms / 1000.0));
                        }
                    }
                }
            }

            // Collect metrics
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

        BenchmarkResult {
            version,
            with_accounts,
            account_owner,
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
            },
            endpoints: [
                EndpointInfo {
                    endpoint: data1.endpoint,
                    plugin_type: meta1.plugin_type,
                    plugin_version: meta1.plugin_version,
                    avg_ping_ms: ping1.as_secs_f64() * 1000.0,
                    total_updates: endpoint1_updates,
                    unique_slots: endpoint1_slots.len() as u64,
                    account_updates: if with_accounts {
                        Some(endpoint1_account_updates)
                    } else {
                        None
                    },
                },
                EndpointInfo {
                    endpoint: data2.endpoint,
                    plugin_type: meta2.plugin_type,
                    plugin_version: meta2.plugin_version,
                    avg_ping_ms: ping2.as_secs_f64() * 1000.0,
                    total_updates: endpoint2_updates,
                    unique_slots: endpoint2_slots.len() as u64,
                    account_updates: if with_accounts {
                        Some(endpoint2_account_updates)
                    } else {
                        None
                    },
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
                account_delay: if with_accounts && !endpoint1_account_delays.is_empty() {
                    Some(Self::percentiles(endpoint1_account_delays))
                } else {
                    None
                },
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
                account_delay: if with_accounts && !endpoint2_account_delays.is_empty() {
                    Some(Self::percentiles(endpoint2_account_delays))
                } else {
                    None
                },
            },
            slots: slot_comparisons,
        }
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
                .0
                .push((update.write_version, update.instant));
        }

        for update in updates2 {
            let key = (
                update.slot,
                update.pubkey.to_string(),
                update.tx_signature.to_string(),
            );
            map.entry(key)
                .or_insert((Vec::new(), Vec::new()))
                .1
                .push((update.write_version, update.instant));
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
    ) -> Vec<AccountUpdateDetail> {
        let mut details = Vec::new();
        if let Some(updates) = updates {
            for update in updates {
                let key = (
                    update.slot,
                    update.pubkey.to_string(),
                    update.tx_signature.to_string(),
                );
                // Calculate delay if matched, using the highest write_version (last update)
                let delay_ms = if let Some((vec1, vec2)) = match_map.get(&key) {
                    match (vec1.last(), vec2.last()) {
                        (Some((_, instant1)), Some((_, instant2))) => {
                            if endpoint_num == 1 {
                                if instant1 < instant2 {
                                    Some(0.0)
                                } else {
                                    Some(instant1.duration_since(*instant2).as_secs_f64() * 1000.0)
                                }
                            } else {
                                if instant2 < instant1 {
                                    Some(0.0)
                                } else {
                                    Some(instant2.duration_since(*instant1).as_secs_f64() * 1000.0)
                                }
                            }
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                details.push(AccountUpdateDetail {
                    pubkey: update.pubkey.to_string(),
                    write_version: update.write_version,
                    tx_signature: update.tx_signature.to_string(),
                    timestamp: Self::to_timestamp_ms(update.system_time),
                    delay_ms,
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
