use {
    crate::{
        Config, GrpcConfig,
        grpc::{GrpcError, SlotCollector},
        processor::{EndpointMetadata, GrpcConfigSummary},
        types::EndpointData,
    },
    std::{sync::Arc, time::Duration},
    tokio::sync::Barrier,
    tracing::info,
};

pub type RunCollectorResult = Result<
    (
        EndpointData,     // data for endpoint1
        EndpointData,     // data for endpoint2
        EndpointMetadata, // metadata for endpoint1
        EndpointMetadata, // metadata for endpoint2
        Duration,         // avg ping for endpoint1
        Duration,         // avg ping for endpoint2
    ),
    GrpcError,
>;

pub struct Collector {
    config: Config,
    endpoint1: String,
    endpoint2: String,
    x_token1: Option<String>,
    x_token2: Option<String>,
    slot_count: usize,
    with_accounts: bool,
    account_owner: Option<String>,
    endpoint1_richat: bool,
    endpoint2_richat: bool,
}

impl Collector {
    pub fn new(
        config: Config,
        endpoint1: String,
        endpoint2: String,
        x_token1: Option<String>,
        x_token2: Option<String>,
        endpoint1_richat: bool,
        endpoint2_richat: bool,
        slot_count: usize,
        with_accounts: bool,
        account_owner: Option<String>,
    ) -> Self {
        Self {
            config,
            endpoint1,
            endpoint2,
            x_token1,
            x_token2,
            slot_count,
            with_accounts,
            account_owner,
            endpoint1_richat,
            endpoint2_richat,
        }
    }

    pub async fn run(self) -> RunCollectorResult {
        let barrier = Arc::new(Barrier::new(2));

        info!("Collecting endpoint versions and ping averages...");

        // Create collectors and get version info
        let collector1 = SlotCollector::new(
            self.make_grpc_config(self.endpoint1.clone(), self.x_token1.clone()),
            self.slot_count,
            self.config.benchmark.buffer_percentage,
            self.config.benchmark.latency_samples,
            self.with_accounts,
            self.account_owner.clone(),
            self.endpoint1_richat,
        )
        .await?;

        let collector2 = SlotCollector::new(
            self.make_grpc_config(self.endpoint2.clone(), self.x_token2.clone()),
            self.slot_count,
            self.config.benchmark.buffer_percentage,
            self.config.benchmark.latency_samples,
            self.with_accounts,
            self.account_owner.clone(),
            self.endpoint2_richat,
        )
        .await?;

        // Capture metadata
        let meta1 = EndpointMetadata {
            plugin_type: if self.endpoint1_richat {
                "Richat".to_string()
            } else {
                "Yellowstone".to_string()
            },
            plugin_version: collector1.version.clone(),
        };

        let meta2 = EndpointMetadata {
            plugin_type: if self.endpoint2_richat {
                "Richat".to_string()
            } else {
                "Yellowstone".to_string()
            },
            plugin_version: collector2.version.clone(),
        };

        let ping1 = collector1.avg_ping;
        let ping2 = collector2.avg_ping;

        // Start synchronized collection
        info!(
            "Starting synchronized collection{}",
            if self.with_accounts {
                " with account updates"
            } else {
                ""
            }
        );

        let (data1, data2) = tokio::join!(
            Self::collect_with_barrier(collector1, Arc::clone(&barrier)),
            Self::collect_with_barrier(collector2, barrier)
        );

        let data1 = data1?;
        let data2 = data2?;

        // Log account collection results
        if self.with_accounts {
            info!(
                "Collected {} account updates from {}",
                data1.account_updates.len(),
                data1.endpoint
            );
            info!(
                "Collected {} account updates from {}",
                data2.account_updates.len(),
                data2.endpoint
            );
        }

        Ok((data1, data2, meta1, meta2, ping1, ping2))
    }

    async fn collect_with_barrier(
        collector: SlotCollector,
        barrier: Arc<Barrier>,
    ) -> Result<EndpointData, GrpcError> {
        barrier.wait().await;
        collector.collect().await
    }

    pub fn get_grpc_config_summary(&self) -> GrpcConfigSummary {
        let g = &self.config.grpc;
        GrpcConfigSummary {
            connect_timeout_ms: g.connect_timeout.as_millis() as u64,
            request_timeout_ms: g.request_timeout.as_millis() as u64,
            max_message_size: g.max_message_size,
            use_tls: g.use_tls,
            http2_adaptive_window: g.http2_adaptive_window,
            initial_connection_window_size: g.initial_connection_window_size,
            initial_stream_window_size: g.initial_stream_window_size,
        }
    }

    fn make_grpc_config(&self, endpoint: String, x_token: Option<String>) -> GrpcConfig {
        let g = &self.config.grpc;
        GrpcConfig {
            endpoint,
            x_token,
            connect_timeout: g.connect_timeout,
            request_timeout: g.request_timeout,
            max_message_size: g.max_message_size,
            use_tls: g.use_tls,
            http2_adaptive_window: g.http2_adaptive_window,
            http2_keep_alive_interval: g.http2_keep_alive_interval,
            initial_connection_window_size: g.initial_connection_window_size,
            initial_stream_window_size: g.initial_stream_window_size,
            tcp_nodelay: g.tcp_nodelay,
            tcp_keepalive: g.tcp_keepalive,
            buffer_size: g.buffer_size,
        }
    }
}
