use {
    crate::{
        Config, GrpcConfig,
        grpc::{GrpcError, SlotCollector},
        types::EndpointData,
    },
    std::sync::Arc,
    tokio::sync::Barrier,
};

pub struct Collector {
    config: Config,
    endpoint1: String,
    endpoint2: String,
    x_token1: Option<String>,
    x_token2: Option<String>,
    slot_count: usize,
}

impl Collector {
    pub fn new(
        config: Config,
        endpoint1: String,
        endpoint2: String,
        x_token1: Option<String>,
        x_token2: Option<String>,
        slot_count: usize,
    ) -> Self {
        Self {
            config,
            endpoint1,
            endpoint2,
            x_token1,
            x_token2,
            slot_count,
        }
    }

    pub async fn run(self) -> Result<(EndpointData, EndpointData), GrpcError> {
        let barrier = Arc::new(Barrier::new(2));
        let buffer = self.config.benchmark.buffer_percentage;

        let collector1 = SlotCollector::new(
            self.make_grpc_config(self.endpoint1.clone(), self.x_token1.clone()),
            self.slot_count,
            buffer,
        )?;

        let collector2 = SlotCollector::new(
            self.make_grpc_config(self.endpoint2.clone(), self.x_token2.clone()),
            self.slot_count,
            buffer,
        )?;

        let (data1, data2) = tokio::join!(
            Self::collect_with_barrier(collector1, Arc::clone(&barrier)),
            Self::collect_with_barrier(collector2, barrier)
        );

        Ok((data1?, data2?))
    }

    async fn collect_with_barrier(
        collector: SlotCollector,
        barrier: Arc<Barrier>,
    ) -> Result<EndpointData, GrpcError> {
        barrier.wait().await;
        collector.collect().await
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
