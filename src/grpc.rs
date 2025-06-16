use {
    crate::{
        EndpointData,
        types::{SlotStatus, SlotUpdate},
    },
    futures::StreamExt,
    std::{
        collections::HashSet,
        time::{Duration, Instant, SystemTime},
        vec,
    },
    tokio::sync::mpsc,
    tracing::{error, info},
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::{
        geyser::{
            SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
            subscribe_update::UpdateOneof,
        },
        tonic::transport::ClientTlsConfig,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum GrpcError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Subscription failed: {0}")]
    SubscriptionFailed(String),

    #[error("Stream error: {0}")]
    StreamError(String),

    #[error("Channel send failed - receiver dropped")]
    ChannelClosed,

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

pub type Result<T> = std::result::Result<T, GrpcError>;

#[derive(Clone)]
pub struct GrpcConfig {
    pub endpoint: String,
    pub x_token: Option<String>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub max_message_size: usize,
    pub use_tls: bool,
    pub http2_adaptive_window: bool,
    pub http2_keep_alive_interval: Option<Duration>,
    pub initial_connection_window_size: Option<u32>,
    pub initial_stream_window_size: Option<u32>,
    pub tcp_nodelay: bool,
    pub tcp_keepalive: Option<Duration>,
    pub buffer_size: Option<usize>,
}

pub struct GrpcClient {
    config: GrpcConfig,
    with_load: bool,
}

impl GrpcClient {
    pub fn new(config: GrpcConfig, with_load: bool) -> Result<Self> {
        if config.endpoint.is_empty() {
            return Err(GrpcError::InvalidConfig("Empty endpoint".into()));
        }

        Ok(Self { config, with_load })
    }

    pub async fn subscribe_slots(&self, tx: mpsc::UnboundedSender<SlotUpdate>) -> Result<()> {
        let mut client = self.connect().await?;

        let mut request = SubscribeRequest {
            slots: [(
                "slots".to_string(),
                SubscribeRequestFilterSlots {
                    // Filter for all slots
                    filter_by_commitment: Some(false), // don't specify commitment
                    interslot_updates: Some(true),     // receive updates for all slots
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        if self.with_load {
            request.accounts.insert(
                "load".to_string(),
                SubscribeRequestFilterAccounts {
                    account: vec![],
                    filters: vec![],
                    nonempty_txn_signature: None,
                    owner: vec!["675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string()], // raydium amm v4 program
                },
            );
        }

        let mut stream = client
            .subscribe_once(request)
            .await
            .map_err(|e| GrpcError::SubscriptionFailed(e.to_string()))?;

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| GrpcError::StreamError(e.to_string()))?;

            if let Some(UpdateOneof::Slot(slot)) = msg.update_oneof {
                let update = SlotUpdate {
                    slot: slot.slot,
                    status: SlotStatus::from(slot.status),
                    instant: Instant::now(),
                    system_time: SystemTime::now(),
                };

                tx.send(update).map_err(|_| GrpcError::ChannelClosed)?;
            }
        }

        Ok(())
    }

    pub async fn measure_latency(&self, samples: usize) -> Result<Vec<Duration>> {
        let mut client = self.connect().await?;
        let mut latencies = Vec::with_capacity(samples);

        for i in 0..samples {
            let start = Instant::now();

            client
                .geyser
                .ping(yellowstone_grpc_proto::geyser::PingRequest { count: i as i32 })
                .await
                .map_err(|e| GrpcError::ConnectionFailed(format!("Ping failed: {}", e)))?;

            latencies.push(start.elapsed());
        }

        Ok(latencies)
    }

    async fn connect(&self) -> Result<GeyserGrpcClient<impl Interceptor>> {
        let mut builder = GeyserGrpcClient::build_from_shared(self.config.endpoint.clone())
            .map_err(|e| GrpcError::ConnectionFailed(e.to_string()))?;

        if let Some(token) = &self.config.x_token {
            builder = builder
                .x_token(Some(token))
                .map_err(|e| GrpcError::ConnectionFailed(e.to_string()))?;
        }

        builder = builder
            .max_decoding_message_size(self.config.max_message_size)
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.request_timeout)
            .http2_adaptive_window(self.config.http2_adaptive_window)
            .tcp_nodelay(self.config.tcp_nodelay);

        if let Some(interval) = self.config.http2_keep_alive_interval {
            builder = builder.http2_keep_alive_interval(interval);
        }

        if let Some(size) = self.config.initial_connection_window_size {
            builder = builder.initial_connection_window_size(size);
        }

        if let Some(size) = self.config.initial_stream_window_size {
            builder = builder.initial_stream_window_size(size);
        }

        if let Some(keepalive) = self.config.tcp_keepalive {
            builder = builder.tcp_keepalive(Some(keepalive));
        }

        if let Some(size) = self.config.buffer_size {
            builder = builder.buffer_size(size);
        }

        if self.config.use_tls {
            builder = builder
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .map_err(|e| GrpcError::ConnectionFailed(format!("TLS config: {}", e)))?;
        }

        builder
            .connect()
            .await
            .map_err(|e| GrpcError::ConnectionFailed(e.to_string()))
    }
}

pub struct SlotCollector {
    client: GrpcClient,
    endpoint_data: EndpointData,
    target_slots: usize,
    buffer_percent: f32,
    pub avg_ping: Duration,
}

impl SlotCollector {
    pub async fn new(
        config: GrpcConfig,
        target_slots: usize,
        buffer_percent: f32,
        latency_samples: usize,
        with_load: bool,
    ) -> Result<Self> {
        let endpoint = config.endpoint.clone();
        let client = GrpcClient::new(config, with_load)?;

        // Measure ping upfront
        let latencies = client.measure_latency(latency_samples).await?;
        let avg_ping = latencies.iter().sum::<Duration>() / latencies.len() as u32;

        tracing::info!(
            "{}: avg ping {:.2}ms",
            endpoint,
            avg_ping.as_secs_f64() * 1000.0
        );

        Ok(Self {
            client,
            endpoint_data: EndpointData::new(endpoint, target_slots, buffer_percent),
            target_slots,
            buffer_percent,
            avg_ping,
        })
    }

    pub async fn collect(mut self) -> Result<EndpointData> {
        let (tx, mut rx) = mpsc::unbounded_channel();

        let endpoint = self.endpoint_data.endpoint.clone();

        let handle = tokio::spawn(async move {
            if let Err(e) = self.client.subscribe_slots(tx).await {
                error!("{} subscription failed: {}", endpoint, e);
            }
        });

        let slots_and_buffer = self.target_slots as f32 * (1.0 + self.buffer_percent);

        // Pre-allocate HashSet with expected capacity
        let pre_allocate_capacity =
            EndpointData::calculate_capacity(self.target_slots, self.buffer_percent);
        let mut seen_slots = HashSet::with_capacity(pre_allocate_capacity);

        let mut last_logged_percent = 0;
        while let Some(update) = rx.recv().await {
            // Track unique slots
            seen_slots.insert(update.slot);

            self.endpoint_data.updates.push(update);

            let percent = (seen_slots.len() as f32 / slots_and_buffer * 100.0).floor() as u32;
            if percent >= last_logged_percent + 10 {
                info!(
                    "{}: {:.0}% of slots seen ({} unique slots)",
                    self.endpoint_data.endpoint,
                    percent,
                    seen_slots.len()
                );
                last_logged_percent = percent;
            }

            // Check if we've collected enough unique slots
            if seen_slots.len() >= slots_and_buffer as usize {
                break;
            }
        }

        handle.abort();
        info!(
            "{}: {} updates, {} unique slots",
            self.endpoint_data.endpoint,
            self.endpoint_data.updates.len(),
            seen_slots.len()
        );

        Ok(self.endpoint_data)
    }
}
