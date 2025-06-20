use {
    crate::types::{EndpointData, SlotStatus, SlotUpdate},
    serde::Serialize,
    std::{
        collections::{HashMap, HashSet},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
};

#[derive(Debug, Serialize)]
pub struct BenchmarkResult {
    pub version: String,           // Yellowstone Thorofare version
    pub with_load: bool,          // Whether --with-load was used
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
}

#[derive(Debug, Serialize)]
pub struct EndpointInfo {
    pub endpoint: String,
    pub plugin_type: String,    // "Yellowstone" or "Richat"
    pub plugin_version: String,
    pub avg_ping_ms: f64,
    pub total_updates: u64,
    pub unique_slots: u64,
}

#[derive(Debug, Serialize)]
pub struct EndpointSummary {
    pub first_shred_delay: Percentiles,
    pub processing_delay: Percentiles,
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
    pub transitions: Vec<Transition>,
    pub durations: StageDurations,
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

type SlotKey = (u64, SlotStatus);

pub struct EndpointMetadata {
    pub plugin_type: String,
    pub plugin_version: String,
}

pub struct Processor;

impl Processor {
    pub fn process(
        version: String,
        with_load: bool,
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

        // Build lookup maps
        let map1 = Self::build_map(data1.updates);
        let map2 = Self::build_map(data2.updates);

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
        let mut endpoint1_download_times = Vec::new();
        let mut endpoint1_replay_times = Vec::new();
        let mut endpoint1_confirmation_times = Vec::new();
        let mut endpoint1_finalization_times = Vec::new();

        let mut endpoint2_first_shred_delays = Vec::new();
        let mut endpoint2_processing_delays = Vec::new();
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

            // Build endpoint-specific data
            let mut endpoint1_detail = Self::build_slot_detail(&map1, slot);
            let mut endpoint2_detail = Self::build_slot_detail(&map2, slot);

            // Add first shred delays to slot details
            endpoint1_detail.first_shred_delay_ms = ep1_first_shred.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.first_shred_delay_ms = ep2_first_shred.map(|d| d.as_secs_f64() * 1000.0);

            // Add processing delays to slot details
            endpoint1_detail.processing_delay_ms = ep1_processing_delay.map(|d| d.as_secs_f64() * 1000.0);
            endpoint2_detail.processing_delay_ms = ep2_processing_delay.map(|d| d.as_secs_f64() * 1000.0);

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

        BenchmarkResult {
            version,
            with_load,
            grpc_config,
            metadata: Metadata {
                total_slots_collected,
                common_slots: common_slots_count,
                compared_slots,
                dropped_slots,
                duration_ms,
                benchmark_start_time: benchmark_start,
            },
            endpoints: [
                EndpointInfo {
                    endpoint: data1.endpoint,
                    plugin_type: meta1.plugin_type,
                    plugin_version: meta1.plugin_version,
                    avg_ping_ms: ping1.as_secs_f64() * 1000.0,
                    total_updates: endpoint1_updates,
                    unique_slots: endpoint1_slots.len() as u64,
                },
                EndpointInfo {
                    endpoint: data2.endpoint,
                    plugin_type: meta2.plugin_type,
                    plugin_version: meta2.plugin_version,
                    avg_ping_ms: ping2.as_secs_f64() * 1000.0,
                    total_updates: endpoint2_updates,
                    unique_slots: endpoint2_slots.len() as u64,
                },
            ],
            endpoint1_summary: EndpointSummary {
                first_shred_delay: Self::percentiles(endpoint1_first_shred_delays),
                processing_delay: Self::percentiles(endpoint1_processing_delays),
                download_time: Self::percentiles(endpoint1_download_times),
                replay_time: Self::percentiles(endpoint1_replay_times),
                confirmation_time: Self::percentiles(endpoint1_confirmation_times),
                finalization_time: Self::percentiles(endpoint1_finalization_times),
            },
            endpoint2_summary: EndpointSummary {
                first_shred_delay: Self::percentiles(endpoint2_first_shred_delays),
                processing_delay: Self::percentiles(endpoint2_processing_delays),
                download_time: Self::percentiles(endpoint2_download_times),
                replay_time: Self::percentiles(endpoint2_replay_times),
                confirmation_time: Self::percentiles(endpoint2_confirmation_times),
                finalization_time: Self::percentiles(endpoint2_finalization_times),
            },
            slots: slot_comparisons,
        }
    }

    fn calc_first_shred_delays(
        map1: &HashMap<SlotKey, SlotUpdate>,
        map2: &HashMap<SlotKey, SlotUpdate>,
        slot: u64,
    ) -> (Option<Duration>, Option<Duration>) {
        let key = (slot, SlotStatus::FirstShredReceived);

        match (map1.get(&key), map2.get(&key)) {
            (Some(u1), Some(u2)) => {
                if u1.instant < u2.instant {
                    // Endpoint1 saw it first, so endpoint2 had the delay
                    let wait_time = u2.instant.duration_since(u1.instant);
                    (Some(Duration::from_secs(0)), Some(wait_time))
                } else if u2.instant < u1.instant {
                    // Endpoint2 saw it first, so endpoint1 had the delay
                    let wait_time = u1.instant.duration_since(u2.instant);
                    (Some(wait_time), Some(Duration::from_secs(0)))
                } else {
                    // Both saw it at the same time
                    (Some(Duration::from_secs(0)), Some(Duration::from_secs(0)))
                }
            }
            _ => (None, None),
        }
    }

    fn calc_processing_delays(
        map1: &HashMap<SlotKey, SlotUpdate>,
        map2: &HashMap<SlotKey, SlotUpdate>,
        slot: u64,
    ) -> (Option<Duration>, Option<Duration>) {
        let key = (slot, SlotStatus::Processed);

        match (map1.get(&key), map2.get(&key)) {
            (Some(u1), Some(u2)) => {
                if u1.instant < u2.instant {
                    // Endpoint1 processed first, so endpoint2 had the delay
                    let wait_time = u2.instant.duration_since(u1.instant);
                    (Some(Duration::from_secs(0)), Some(wait_time))
                } else if u2.instant < u1.instant {
                    // Endpoint2 processed first, so endpoint1 had the delay
                    let wait_time = u1.instant.duration_since(u2.instant);
                    (Some(wait_time), Some(Duration::from_secs(0)))
                } else {
                    // Both processed at the same time
                    (Some(Duration::from_secs(0)), Some(Duration::from_secs(0)))
                }
            }
            _ => (None, None),
        }
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

        // Collect transitions
        let mut transitions = Vec::new();
        for status in &statuses {
            if let Some(update) = map.get(&(slot, *status)) {
                transitions.push(Transition {
                    status: format!("{:?}", status),
                    timestamp: Self::to_timestamp_ms(update.system_time),
                });
            }
        }

        // Calculate durations - unwrap is safe here because we checked all statuses exist
        let durations = StageDurations {
            download_ms: Self::calc_download(map, slot).unwrap().as_secs_f64() * 1000.0,
            replay_ms: Self::calc_replay(map, slot).unwrap().as_secs_f64() * 1000.0,
            confirmation_ms: Self::calc_confirmation(map, slot).unwrap().as_secs_f64() * 1000.0,
            finalization_ms: Self::calc_finalization(map, slot).unwrap().as_secs_f64() * 1000.0,
        };

        SlotDetail {
            first_shred_delay_ms: None, // Will be filled by caller
            processing_delay_ms: None, // Will be filled by caller
            transitions,
            durations,
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