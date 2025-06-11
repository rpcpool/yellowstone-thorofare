use {
    anyhow::Result,
    clap::Parser,
    std::time::Duration,
    tokio::sync::mpsc,
    tracing::{error, info, warn},
    yellowstone_grpc_bench::{GrpcClient, GrpcConfig, SlotStatus},
};

#[derive(Parser)]
struct Args {
    #[clap(long, default_value = "http://localhost:10000")]
    endpoint: String,

    #[clap(long)]
    x_token: Option<String>,

    #[clap(long, default_value = "ping")]
    mode: String, // "ping" or "slots"

    #[clap(long, default_value = "info")]
    log_level: String,
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
        // might want to enable if we add multiple threads
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();

    let localhost = args.endpoint.starts_with("http");

    let config = GrpcConfig {
        endpoint: args.endpoint.clone(),
        x_token: args.x_token,
        connect_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(10),
        max_message_size: 1024 * 1024,
        use_tls: !localhost,
        http2_adaptive_window: false,
        http2_keep_alive_interval: None,
        initial_connection_window_size: None,
        initial_stream_window_size: None,
        tcp_nodelay: true,
        tcp_keepalive: None,
        buffer_size: None,
    };

    info!("Connecting to {}", args.endpoint);
    let client = GrpcClient::new(config)?;

    match args.mode.as_str() {
        "ping" => {
            info!("Measuring latency with 5 samples...");
            match client.measure_latency(5).await {
                Ok(latencies) => {
                    for (i, latency) in latencies.iter().enumerate() {
                        info!("Ping {}: {:?}", i + 1, latency);
                    }
                    let avg = latencies.iter().sum::<Duration>() / latencies.len() as u32;
                    info!("Average latency: {:?}", avg);
                }
                Err(e) => {
                    error!("Ping failed: {}", e);
                    return Err(e.into());
                }
            }
        }
        "slots" => {
            info!("Subscribing to slot updates (Ctrl+C to stop)...");
            let (tx, mut rx) = mpsc::unbounded_channel();

            // Spawn subscription task
            tokio::spawn(async move {
                if let Err(e) = client.subscribe_slots(tx).await {
                    error!("Subscription error: {}", e);
                }
            });

            // Process updates
            let mut count = 0;
            while let Some(update) = rx.recv().await {
                count += 1;
                info!(
                    slot = update.slot,
                    status = ?update.status,
                    instant = ?update.instant,
                    timestamp = ?update.timestamp,
                    "Slot update #{}", count
                );

                // Log transitions for debugging
                match update.status {
                    SlotStatus::FirstShredReceived => {
                        info!("First shred for slot {}", update.slot);
                    }
                    SlotStatus::Completed => {
                        info!("Slot {} completed", update.slot);
                    }
                    SlotStatus::Dead => {
                        warn!("Slot {} marked dead", update.slot);
                    }
                    _ => {}
                }
            }
        }
        mode => {
            error!("Unknown mode: {}", mode);
            std::process::exit(1);
        }
    }

    Ok(())
}
