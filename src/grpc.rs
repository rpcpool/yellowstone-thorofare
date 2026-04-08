use {
    crate::{
        EndpointData,
        richat::{RichatSubscriber, richat_client_from_config},
        types::{AccountUpdate, SlotStatus, SlotUpdate, TransactionUpdate},
    },
    futures::StreamExt,
    richat_proto::{
        geyser::subscribe_update::UpdateOneof as RichatUpdateOneof, richat::RichatFilter,
    },
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::HashSet,
        time::{Duration, Instant, SystemTime},
    },
    tokio::sync::mpsc,
    tracing::{error, info, warn},
    yellowstone_grpc_client::{GeyserGrpcClient, Interceptor},
    yellowstone_grpc_proto::{
        geyser::{
            SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions,
            subscribe_update::UpdateOneof as YellowstoneUpdateOneof,
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

#[derive(Clone)]
pub struct GrpcClient {
    config: GrpcConfig,
    with_accounts: bool,
    account_owner: Option<String>,
    with_transactions: bool,
}

impl GrpcClient {
    pub fn new(
        config: GrpcConfig,
        with_accounts: bool,
        account_owner: Option<String>,
        with_transactions: bool,
    ) -> Result<Self> {
        if config.endpoint.is_empty() {
            return Err(GrpcError::InvalidConfig("Empty endpoint".into()));
        }

        Ok(Self {
            config,
            with_accounts,
            account_owner,
            with_transactions,
        })
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
            let received_at = Instant::now();
            let received_system_time = SystemTime::now();

            if let Some(YellowstoneUpdateOneof::Slot(slot)) = msg.update_oneof {
                let update = SlotUpdate {
                    slot: slot.slot,
                    status: SlotStatus::from(slot.status),
                    instant: received_at,
                    system_time: received_system_time,
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
                SubscribeRequestFilterAccounts {
                    owner: self.account_owner.clone().into_iter().collect(),
                    nonempty_txn_signature: Some(true),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            commitment: Some(0), // Processed
            ..Default::default()
        };

        let mut stream = client
            .subscribe_once(request)
            .await
            .map_err(|e| GrpcError::SubscriptionFailed(e.to_string()))?;

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| GrpcError::StreamError(e.to_string()))?;
            let received_at = Instant::now();
            let received_system_time = SystemTime::now();

            if let Some(YellowstoneUpdateOneof::Account(account)) = msg.update_oneof {
                let Some(account_info) = account.account.as_ref() else {
                    continue;
                };
                let Ok(pubkey) = Pubkey::try_from(account_info.pubkey.as_slice()) else {
                    continue;
                };
                let Some(txn_signature_bytes) = account_info.txn_signature.as_ref() else {
                    continue;
                };
                let Ok(tx_signature) = Signature::try_from(txn_signature_bytes.as_slice()) else {
                    continue;
                };

                let update = AccountUpdate {
                    slot: account.slot,
                    pubkey,
                    write_version: account_info.write_version,
                    tx_signature,
                    instant: received_at,
                    system_time: received_system_time,
                };

                tx.send(update).map_err(|_| GrpcError::ChannelClosed)?;
            }
        }

        Ok(())
    }

    pub async fn subscribe_transactions(
        &self,
        tx: mpsc::UnboundedSender<TransactionUpdate>,
    ) -> Result<()> {
        let mut client = self.connect().await?;

        let request = SubscribeRequest {
            transactions: [(
                "".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: Some(false),
                    failed: None,
                    signature: None,
                    account_include: vec![],
                    account_exclude: vec![],
                    account_required: vec![],
                },
            )]
            .into_iter()
            .collect(),
            commitment: Some(0), // Processed
            ..Default::default()
        };

        let mut stream = client
            .subscribe_once(request)
            .await
            .map_err(|e| GrpcError::SubscriptionFailed(e.to_string()))?;

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| GrpcError::StreamError(e.to_string()))?;
            let received_at = Instant::now();
            let received_system_time = SystemTime::now();

            if let Some(YellowstoneUpdateOneof::Transaction(tx_update)) = msg.update_oneof {
                let Some(tx_info) = tx_update.transaction.as_ref() else {
                    continue;
                };
                let Ok(signature) = Signature::try_from(tx_info.signature.as_slice()) else {
                    continue;
                };

                let update = TransactionUpdate {
                    slot: tx_update.slot,
                    signature,
                    instant: received_at,
                    system_time: received_system_time,
                };

                tx.send(update).map_err(|_| GrpcError::ChannelClosed)?;
            }
        }

        Ok(())
    }

    pub async fn subscribe_transactions_richat(
        &self,
        _tx: mpsc::UnboundedSender<TransactionUpdate>,
    ) -> Result<()> {
        todo!("Richat transaction subscription not yet implemented")
    }

    pub async fn subscribe_slots_richat(
        &self,
        tx: mpsc::UnboundedSender<SlotUpdate>,
    ) -> Result<()> {
        let request = richat_proto::richat::GrpcSubscribeRequest {
            filter: Some(RichatFilter {
                disable_accounts: true, // Only want slots
                disable_entries: true,
                disable_transactions: true,
            }),
            ..Default::default()
        };
        let mut stream = RichatSubscriber::spawn_from_config(request, self.config.clone())
            .await
            .map_err(|e| GrpcError::ConnectionFailed(e.to_string()))?;

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| GrpcError::StreamError(e.to_string()))?;
            let received_at = Instant::now();
            let received_system_time = SystemTime::now();

            if let Some(RichatUpdateOneof::Slot(slot)) = msg.update_oneof {
                let update = SlotUpdate {
                    slot: slot.slot,
                    status: SlotStatus::from(slot.status),
                    instant: received_at,
                    system_time: received_system_time,
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
                disable_accounts: false, // Want accounts
                disable_entries: true,
                disable_transactions: true,
            }),
            ..Default::default()
        };

        let mut stream = RichatSubscriber::spawn_from_config(request, self.config.clone())
            .await
            .map_err(|e| GrpcError::ConnectionFailed(e.to_string()))?;

        while let Some(msg) = stream.next().await {
            let msg = msg.map_err(|e| GrpcError::StreamError(e.to_string()))?;
            let received_at = Instant::now();
            let received_system_time = SystemTime::now();

            if let Some(RichatUpdateOneof::Account(account)) = msg.update_oneof {
                let Some(account_info) = account.account.as_ref() else {
                    continue;
                };
                let Ok(pubkey) = Pubkey::try_from(account_info.pubkey.as_slice()) else {
                    continue;
                };
                let Some(txn_signature_bytes) = account_info.txn_signature.as_ref() else {
                    continue;
                };
                let Ok(tx_signature) = Signature::try_from(txn_signature_bytes.as_slice()) else {
                    continue;
                };

                let update = AccountUpdate {
                    slot: account.slot,
                    pubkey,
                    write_version: account_info.write_version,
                    tx_signature,
                    instant: received_at,
                    system_time: received_system_time,
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
        let mut client = richat_client_from_config(self.config.clone())
            .await
            .map_err(|e| {
                GrpcError::ConnectionFailed(format!("Richat client connection failed: {}", e))
            })?;

        let response = client
            .get_version()
            .await
            .map_err(|e| GrpcError::ConnectionFailed(format!("Version request failed: {}", e)))?;

        Ok(response.version)
    }

    pub async fn measure_latency_richat(&self, samples: usize) -> Result<Vec<Duration>> {
        let mut client = richat_client_from_config(self.config.clone())
            .await
            .map_err(|e| {
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
    const UNKNOWN_VERSION: &str = "unknown";

    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        config: GrpcConfig,
        target_slots: usize,
        buffer_percent: f32,
        latency_samples: usize,
        with_accounts: bool,
        account_owner: Option<String>,
        with_transactions: bool,
        richat: bool,
    ) -> Result<Self> {
        let endpoint = config.endpoint.clone();
        let client = GrpcClient::new(config, with_accounts, account_owner, with_transactions)?;

        // Version is metadata only, so don't fail the whole benchmark if it isn't exposed.
        let version = if richat {
            info!("Getting Richat version...");
            match client.get_version_richat().await {
                Ok(version) => version,
                Err(error) => {
                    warn!(
                        "{}: failed to get Richat version ({}); using {}",
                        endpoint,
                        error,
                        Self::UNKNOWN_VERSION
                    );
                    Self::UNKNOWN_VERSION.to_string()
                }
            }
        } else {
            info!("Getting Yellowstone version...");
            match client.get_version().await {
                Ok(version) => version,
                Err(error) => {
                    warn!(
                        "{}: failed to get Yellowstone version ({}); using {}",
                        endpoint,
                        error,
                        Self::UNKNOWN_VERSION
                    );
                    Self::UNKNOWN_VERSION.to_string()
                }
            }
        };

        info!("{}: version {}", endpoint, version);

        // Ping is also metadata, so keep going if the probe isn't available.
        let avg_ping = if latency_samples > 0 {
            let latencies = if richat {
                info!(
                    "Measuring Richat latency with {} samples...",
                    latency_samples
                );
                client.measure_latency_richat(latency_samples).await
            } else {
                info!(
                    "Measuring Geyser latency with {} samples...",
                    latency_samples
                );
                client.measure_latency(latency_samples).await
            };

            match latencies {
                Ok(latencies) => latencies.iter().sum::<Duration>() / latencies.len() as u32,
                Err(error) => {
                    warn!(
                        "{}: failed to measure latency ({}); using 0ms",
                        endpoint, error
                    );
                    Duration::ZERO
                }
            }
        } else {
            Duration::ZERO
        };

        info!(
            "{}: avg ping {:.2}ms",
            endpoint,
            avg_ping.as_micros() as f64 / 1000.0
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
        let (tx_tx, mut tx_rx) = mpsc::unbounded_channel();

        let endpoint = self.endpoint_data.endpoint.clone();
        let richat = self.richat;
        let with_accounts = self.client.with_accounts;
        let with_transactions = self.client.with_transactions;

        // Spawn slot collector
        let slot_handle = {
            let client = self.client.clone();
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                if richat {
                    if let Err(e) = client.subscribe_slots_richat(slot_tx).await {
                        error!("{} slot subscription failed: {}", endpoint, e);
                    }
                } else if let Err(e) = client.subscribe_slots(slot_tx).await {
                    error!("{} slot subscription failed: {}", endpoint, e);
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
                } else if let Err(e) = client.subscribe_accounts(account_tx).await {
                    error!("{} account subscription failed: {}", endpoint, e);
                }
            }))
        } else {
            None
        };

        // Spawn transaction collector if needed
        let tx_handle = if with_transactions {
            let client = self.client.clone();
            let endpoint = endpoint.clone();
            Some(tokio::spawn(async move {
                if richat {
                    if let Err(e) = client.subscribe_transactions_richat(tx_tx).await {
                        error!("{} transaction subscription failed: {}", endpoint, e);
                    }
                } else if let Err(e) = client.subscribe_transactions(tx_tx).await {
                    error!("{} transaction subscription failed: {}", endpoint, e);
                }
            }))
        } else {
            None
        };

        let target_slot_count = (self.target_slots as f32 * (1.0 + self.buffer_percent)) as usize;
        let mut seen_slots = HashSet::with_capacity(target_slot_count);
        let mut seen_accounts = HashSet::new();
        let mut seen_transactions = HashSet::new();

        let mut last_logged_percent = 0;

        // Collect until we have enough slots
        loop {
            tokio::select! {
                Some(update) = slot_rx.recv() => {
                    seen_slots.insert(update.slot);
                    self.endpoint_data.updates.push(update);

                    let percent =
                        (seen_slots.len() as f32 / target_slot_count as f32 * 100.0).floor() as u32;
                    if percent >= last_logged_percent + 10 {
                        info!(
                            "{}: {:.0}% of slots seen ({} unique slots)",
                            self.endpoint_data.endpoint,
                            percent,
                            seen_slots.len()
                        );
                        last_logged_percent = percent;
                    }

                    if seen_slots.len() >= target_slot_count {
                        break;
                    }
                }
                Some(account) = account_rx.recv() => {
                    let key = (
                        account.slot,
                        account.pubkey,
                        account.tx_signature,
                    );
                    if seen_accounts.insert(key) {
                        self.endpoint_data.account_updates.push(account);
                    }
                }
                Some(tx_update) = tx_rx.recv() => {
                    if seen_transactions.insert(tx_update.signature) {
                        self.endpoint_data.transaction_updates.push(tx_update);
                    }
                }
                else => break,
            }
        }

        slot_handle.abort();
        if let Some(handle) = account_handle {
            handle.abort();
        }
        if let Some(handle) = tx_handle {
            handle.abort();
        }

        info!(
            "{}: {} slot updates, {} unique slots, {} account updates, {} transaction updates",
            self.endpoint_data.endpoint,
            self.endpoint_data.updates.len(),
            seen_slots.len(),
            self.endpoint_data.account_updates.len(),
            self.endpoint_data.transaction_updates.len(),
        );

        Ok(self.endpoint_data)
    }
}
