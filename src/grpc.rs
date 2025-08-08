use {
    crate::{
        richat::{richat_client_from_config, RichatSubscriber}, 
        types::{SlotStatus, SlotUpdate, AccountUpdate}, 
        EndpointData
    }, 
    futures::StreamExt, 
    richat_proto::richat::RichatFilter, 
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::HashSet,
        time::{Duration, Instant, SystemTime},
    }, 
    tokio::sync::mpsc, 
    tracing::{error, info}, 
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor}, 
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots
        },
        tonic::transport::ClientTlsConfig,
    }
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

#[derive(Clone)]
pub struct GrpcClient {
    config: GrpcConfig,
    with_accounts: bool,
}

impl GrpcClient {
    pub fn new(config: GrpcConfig, with_accounts: bool) -> Result<Self> {
        if config.endpoint.is_empty() {
            return Err(GrpcError::InvalidConfig("Empty endpoint".into()));
        }

        Ok(Self { config, with_accounts })
    }

    pub async fn subscribe_slots(&self, tx: mpsc::UnboundedSender<SlotUpdate>) -> Result<()> {
        let mut client = self.connect().await?;

        let request = SubscribeRequest {
            slots: [(
                "".to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(false), // don't specify commitment
                    interslot_updates: Some(true),     // receive updates for all slots
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };

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

    pub async fn subscribe_accounts(&self, tx: mpsc::UnboundedSender<AccountUpdate>) -> Result<()> {
        let mut client = self.connect().await?;

        let request = SubscribeRequest {
            accounts: [(
                "".to_string(),
                SubscribeRequestFilterAccounts::default(),
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        let mut stream = client
            .subscribe_once(request)
            .await
            .map_err(|e| GrpcError::SubscriptionFailed(e.to_string()))?;

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| GrpcError::StreamError(e.to_string()))?;

            if let Some(UpdateOneof::Account(account)) = msg.update_oneof {
                let pubkey = Pubkey::try_from(account.account.as_ref().map(|a| a.pubkey.as_slice()).unwrap_or_default())
                    .unwrap_or_default();
                
                let tx_signature = account.account.as_ref()
                    .and_then(|a| a.txn_signature.as_ref())
                    .and_then(|sig| Signature::try_from(sig.as_slice()).ok())
                    .unwrap_or_default();

                let update = AccountUpdate {
                    slot: account.slot,
                    pubkey,
                    write_version: account.account.as_ref().map(|a| a.write_version).unwrap_or(0),
                    tx_signature,
                    instant: Instant::now(),
                    system_time: SystemTime::now(),
                };

                tx.send(update).map_err(|_| GrpcError::ChannelClosed)?;
            }
        }

        Ok(())
    }

    pub async fn subscribe_slots_richat(
        &self,
        tx: mpsc::UnboundedSender<SlotUpdate>,
    ) -> Result<()> {
        let request = richat_proto::richat::GrpcSubscribeRequest {
            filter: Some(RichatFilter {
                disable_accounts: true,  // Only want slots
                disable_entries: true,
                disable_transactions: true,
            }),
            ..Default::default()
        };
        let mut stream = RichatSubscriber::spawn_from_config(
            request,
            self.config.clone()
        ).await.map_err(|e| GrpcError::ConnectionFailed(e.to_string()))?;

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

    pub async fn subscribe_accounts_richat(
        &self,
        tx: mpsc::UnboundedSender<AccountUpdate>,
    ) -> Result<()> {
        let request = richat_proto::richat::GrpcSubscribeRequest {
            filter: Some(RichatFilter {
                disable_accounts: false,  // Want accounts
                disable_entries: true,
                disable_transactions: true,
            }),
            ..Default::default()
        };
        
        let mut stream = RichatSubscriber::spawn_from_config(
            request,
            self.config.clone()
        ).await.map_err(|e| GrpcError::ConnectionFailed(e.to_string()))?;

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| GrpcError::StreamError(e.to_string()))?;
            
            if let Some(UpdateOneof::Account(account)) = msg.update_oneof {
                let pubkey = Pubkey::try_from(account.account.as_ref().map(|a| a.pubkey.as_slice()).unwrap_or_default())
                    .unwrap_or_default();
                
                let tx_signature = account.account.as_ref()
                    .and_then(|a| a.txn_signature.as_ref())
                    .and_then(|sig| Signature::try_from(sig.as_slice()).ok())
                    .unwrap_or_default();

                let update = AccountUpdate {
                    slot: account.slot,
                    pubkey,
                    write_version: account.account.as_ref().map(|a| a.write_version).unwrap_or(0),
                    tx_signature,
                    instant: Instant::now(),
                    system_time: SystemTime::now(),
                };

                tx.send(update).map_err(|_| GrpcError::ChannelClosed)?;
            }
        }

        Ok(())
    }

    pub async fn get_version(&self) -> Result<String> {
        let mut client = self.connect().await?;
        
        let response = client
            .geyser
            .get_version(yellowstone_grpc_proto::geyser::GetVersionRequest::default())
            .await
            .map_err(|e| GrpcError::ConnectionFailed(format!("Version request failed: {}", e)))?;
            
        Ok(response.into_inner().version)
    }

    pub async fn get_version_richat(&self) -> Result<String> {
        let mut client = richat_client_from_config(self.config.clone()).await.map_err(|e| {
            GrpcError::ConnectionFailed(format!("Richat client connection failed: {}", e))
        })?;
        
        let response = client
            .get_version()
            .await
            .map_err(|e| GrpcError::ConnectionFailed(format!("Version request failed: {}", e)))?;
            
        Ok(response.version)
    }

    pub async fn measure_latency_richat(&self, samples: usize) -> Result<Vec<Duration>> {
        let mut client = richat_client_from_config(self.config.clone()).await.map_err(|e| {
            GrpcError::ConnectionFailed(format!("Richat client connection failed: {}", e))
        })?;
        let mut latencies = Vec::with_capacity(samples);

        for _ in 0..samples {
            let start = Instant::now();

            client
                .get_version()
                .await
                .map_err(|e| GrpcError::ConnectionFailed(format!("Ping failed: {}", e)))?;

            latencies.push(start.elapsed());
        }

        Ok(latencies)
    }

    pub async fn measure_latency(&self, samples: usize) -> Result<Vec<Duration>> {
        let mut client = self.connect().await?;
        let mut latencies = Vec::with_capacity(samples);

        for _ in 0..samples {
            let start = Instant::now();

            client
                .geyser
                .get_version(yellowstone_grpc_proto::geyser::GetVersionRequest::default())
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
    pub version: String,
    richat: bool,
}

impl SlotCollector {
    pub async fn new(
        config: GrpcConfig,
        target_slots: usize,
        buffer_percent: f32,
        latency_samples: usize,
        with_accounts: bool,
        richat: bool,
    ) -> Result<Self> {
        let endpoint = config.endpoint.clone();
        let client = GrpcClient::new(config, with_accounts)?;

        // Get version first
        let version = if richat {
            info!("Getting Richat version...");
            client.get_version_richat().await?
        } else {
            info!("Getting Yellowstone version...");
            client.get_version().await?
        };
        
        info!("{}: version {}", endpoint, version);

        // Measure ping
        let avg_ping = if latency_samples > 0 {
            let latencies = if richat {
                info!("Measuring Richat latency with {} samples...", latency_samples);
                client.measure_latency_richat(latency_samples).await?
            } else {
                info!("Measuring Geyser latency with {} samples...", latency_samples);
                client.measure_latency(latency_samples).await?
            };
            latencies.iter().sum::<Duration>() / latencies.len() as u32
        } else {
            Duration::ZERO
        };

        info!(
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
            version,
            richat,
        })
    }

    pub async fn collect(mut self) -> Result<EndpointData> {
        let (slot_tx, mut slot_rx) = mpsc::unbounded_channel();
        let (account_tx, mut account_rx) = mpsc::unbounded_channel();

        let endpoint = self.endpoint_data.endpoint.clone();
        let richat = self.richat;
        let with_accounts = self.client.with_accounts;

        // Spawn slot collector
        let slot_handle = {
            let client = self.client.clone();
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                if richat {
                    if let Err(e) = client.subscribe_slots_richat(slot_tx).await {
                        error!("{} slot subscription failed: {}", endpoint, e);
                    }
                } else {
                    if let Err(e) = client.subscribe_slots(slot_tx).await {
                        error!("{} slot subscription failed: {}", endpoint, e);
                    }
                }
            })
        };

        // Spawn account collector if needed
        let account_handle = if with_accounts {
            let client = self.client.clone();
            let endpoint = endpoint.clone();
            Some(tokio::spawn(async move {
                if richat {
                    if let Err(e) = client.subscribe_accounts_richat(account_tx).await {
                        error!("{} account subscription failed: {}", endpoint, e);
                    }
                } else {
                    if let Err(e) = client.subscribe_accounts(account_tx).await {
                        error!("{} account subscription failed: {}", endpoint, e);
                    }
                }
            }))
        } else {
            None
        };

        let slots_and_buffer = self.target_slots as f32 * (1.0 + self.buffer_percent);
        let pre_allocate_capacity =
            EndpointData::calculate_capacity(self.target_slots, self.buffer_percent);
        let mut seen_slots = HashSet::with_capacity(pre_allocate_capacity);

        let mut last_logged_percent = 0;
        
        // Collect until we have enough slots
        loop {
            tokio::select! {
                Some(update) = slot_rx.recv() => {
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

                    if seen_slots.len() >= slots_and_buffer as usize {
                        break;
                    }
                }
                Some(account) = account_rx.recv() => {
                    self.endpoint_data.account_updates.push(account);
                }
                else => break,
            }
        }

        slot_handle.abort();
        if let Some(handle) = account_handle {
            handle.abort();
        }
        
        info!(
            "{}: {} slot updates, {} unique slots, {} account updates",
            self.endpoint_data.endpoint,
            self.endpoint_data.updates.len(),
            seen_slots.len(),
            self.endpoint_data.account_updates.len()
        );

        Ok(self.endpoint_data)
    }
}