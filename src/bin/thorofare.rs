use {
    anyhow::Result,
    clap::Parser,
    std::{fs, time::Instant},
    tracing::{error, info},
    yellowstone_thorofare::{Collector, Config, Processor},
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser)]
#[clap(name = "grpc-bench", about = "Benchmark gRPC endpoints")]
struct Args {
    /// First endpoint to benchmark
    #[clap(long)]
    endpoint1: String,

    /// Second endpoint to benchmark
    #[clap(long)]
    endpoint2: String,

    /// X-Token for first endpoint
    #[clap(long)]
    x_token1: Option<String>,

    /// X-Token for second endpoint
    #[clap(long)]
    x_token2: Option<String>,

    /// Use Richat interface for endpoint1
    #[clap(long)]
    endpoint1_richat: bool,

    /// Use Richat interface for endpoint2
    #[clap(long)]
    endpoint2_richat: bool,

    /// Number of slots to collect
    #[clap(long, default_value = "1000")]
    slots: usize,

    /// Config file path
    #[clap(long, default_value = "config.toml")]
    config: String,

    /// Output JSON file
    #[clap(long, default_value = "benchmark_results.json")]
    output: String,

    /// Log level
    #[clap(long, default_value = "info")]
    log_level: String,

    /// Collect all account updates for comparison
    #[clap(long)]
    with_accounts: bool,

    /// Collect all entry updates for comparison
    #[clap(short, long)]
    with_entries: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Setup tracing
    let filter = args
        .log_level
        .parse::<tracing_subscriber::filter::LevelFilter>()
        .unwrap_or(tracing_subscriber::filter::LevelFilter::INFO);

    tracing_subscriber::fmt()
        .with_max_level(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    // Load config or use defaults
    let config = match Config::load(&args.config) {
        Ok(cfg) => {
            info!("Loaded config from {}", args.config);
            cfg
        }
        Err(_) => {
            info!("Using default config");
            Config::default()
        }
    };

    info!("Starting Yellowstone-Thorofare v{}", VERSION);
    info!("With Accounts: {}", args.with_accounts);
    info!("Endpoint 1: {} ({})", args.endpoint1, if args.endpoint1_richat { "Richat" } else { "Yellowstone" });
    info!("Endpoint 2: {} ({})", args.endpoint2, if args.endpoint2_richat { "Richat" } else { "Yellowstone" });
    info!("Target slots: {}", args.slots);

    // Start benchmark
    let start_time = Instant::now();

    let collector = Collector::new(
        config,
        args.endpoint1,
        args.endpoint2,
        args.x_token1,
        args.x_token2,
        args.endpoint1_richat,
        args.endpoint2_richat,
        args.slots,
        args.with_accounts,
        args.with_entries,
    );

    info!("Starting data collection...");

    let grpc_config_summary = collector.get_grpc_config_summary();

    match collector.run().await {
        Ok((data1, data2, meta1, meta2, ping1, ping2)) => {
            info!("Collection complete, processing results...");

            let result = Processor::process(
                VERSION.to_string(),
                args.with_accounts,
                grpc_config_summary,
                data1,
                data2,
                meta1,
                meta2,
                ping1,
                ping2,
                start_time,
            );

            // Save to JSON
            let json = serde_json::to_string_pretty(&result)?;
            fs::write(&args.output, json)?;

            info!("Results saved to {}", args.output);

            // Print summary
            info!("\n=== BENCHMARK SUMMARY ===");
            info!("Tool version: {}", VERSION);
            info!("With Accounts: {}", args.with_accounts);
            info!(
                "Total slots collected: {}",
                result.metadata.total_slots_collected
            );
            info!("Common slots: {}", result.metadata.common_slots);
            info!("Compared slots: {}", result.metadata.compared_slots);
            info!("Dropped slots: {}", result.metadata.dropped_slots);
            info!("Duration: {}ms", result.metadata.duration_ms);

            info!(
                "\nEndpoint 1: {} ({} v{}) (ping: {:.2}ms)",
                result.endpoints[0].endpoint, 
                result.endpoints[0].plugin_type,
                result.endpoints[0].plugin_version,
                result.endpoints[0].avg_ping_ms
            );
            info!("  Total updates: {}", result.endpoints[0].total_updates);
            info!("  Unique slots: {}", result.endpoints[0].unique_slots);

            info!(
                "\nEndpoint 2: {} ({} v{}) (ping: {:.2}ms)",
                result.endpoints[1].endpoint,
                result.endpoints[1].plugin_type, 
                result.endpoints[1].plugin_version,
                result.endpoints[1].avg_ping_ms
            );
            info!("  Total updates: {}", result.endpoints[1].total_updates);
            info!("  Unique slots: {}", result.endpoints[1].unique_slots);

            info!("\n=== ENDPOINT 1 PERFORMANCE (ms) ===");
            info!(
                "First Shred Delay: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint1_summary.first_shred_delay.p50,
                result.endpoint1_summary.first_shred_delay.p90,
                result.endpoint1_summary.first_shred_delay.p99
            );
            info!(
                "Processing Delay: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint1_summary.processing_delay.p50,
                result.endpoint1_summary.processing_delay.p90,
                result.endpoint1_summary.processing_delay.p99
            );
            info!(
                "Confirmation Delay: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint1_summary.confirmation_delay.p50,
                result.endpoint1_summary.confirmation_delay.p90,
                result.endpoint1_summary.confirmation_delay.p99
            );
            info!(
                "Finalization Delay: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint1_summary.finalization_delay.p50,
                result.endpoint1_summary.finalization_delay.p90,
                result.endpoint1_summary.finalization_delay.p99
            );
            info!(
                "Download Time: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint1_summary.download_time.p50,
                result.endpoint1_summary.download_time.p90,
                result.endpoint1_summary.download_time.p99
            );
            info!(
                "Replay Time: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint1_summary.replay_time.p50,
                result.endpoint1_summary.replay_time.p90,
                result.endpoint1_summary.replay_time.p99
            );
            info!(
                "Confirmation Time: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint1_summary.confirmation_time.p50,
                result.endpoint1_summary.confirmation_time.p90,
                result.endpoint1_summary.confirmation_time.p99
            );
            info!(
                "Finalization Time: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint1_summary.finalization_time.p50,
                result.endpoint1_summary.finalization_time.p90,
                result.endpoint1_summary.finalization_time.p99
            );

            info!("\n=== ENDPOINT 2 PERFORMANCE (ms) ===");
            info!(
                "First Shred Delay: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint2_summary.first_shred_delay.p50,
                result.endpoint2_summary.first_shred_delay.p90,
                result.endpoint2_summary.first_shred_delay.p99
            );
            info!(
                "Processing Delay: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint2_summary.processing_delay.p50,
                result.endpoint2_summary.processing_delay.p90,
                result.endpoint2_summary.processing_delay.p99
            );
            info!(
                "Download Time: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint2_summary.download_time.p50,
                result.endpoint2_summary.download_time.p90,
                result.endpoint2_summary.download_time.p99
            );
            info!(
                "Replay Time: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint2_summary.replay_time.p50,
                result.endpoint2_summary.replay_time.p90,
                result.endpoint2_summary.replay_time.p99
            );
            info!(
                "Confirmation Time: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint2_summary.confirmation_time.p50,
                result.endpoint2_summary.confirmation_time.p90,
                result.endpoint2_summary.confirmation_time.p99
            );
            info!(
                "Finalization Time: p50={:.2}, p90={:.2}, p99={:.2}",
                result.endpoint2_summary.finalization_time.p50,
                result.endpoint2_summary.finalization_time.p90,
                result.endpoint2_summary.finalization_time.p99
            );
        }
        Err(e) => {
            error!("Benchmark failed: {}", e);
            std::process::exit(1);
        }
    }

    Ok(())
}