use {
    crate::GrpcConfig,
    richat_client::{grpc::GrpcClient, stream::SubscribeStream},
    richat_proto::richat::GrpcSubscribeRequest,
    tonic::service::Interceptor,
    yellowstone_grpc_client::ClientTlsConfig,
};

#[derive(Debug, thiserror::Error)]
pub enum RichatSubscribeError {
    #[error(transparent)]
    TransportError(#[from] tonic::transport::Error),
    #[error(transparent)]
    Status(#[from] tonic::Status),
}

pub async fn richat_client_from_config(
    config: GrpcConfig,
) -> Result<GrpcClient<impl Interceptor>, RichatSubscribeError> {
    let ret = GrpcClient::build_from_shared(config.endpoint)?
        .x_token(config.x_token.map(|token| token.into_bytes()))
        .expect("Failed to set x_token")
        .tls_config(ClientTlsConfig::new().with_native_roots())
        .expect("Failed to set TLS config")
        .max_decoding_message_size(config.max_message_size)
        .connect_timeout(config.connect_timeout)
        .http2_adaptive_window(config.http2_adaptive_window)
        .tcp_keepalive(config.tcp_keepalive)
        .buffer_size(config.buffer_size)
        .initial_connection_window_size(config.initial_connection_window_size)
        .initial_stream_window_size(config.initial_stream_window_size)
        .tcp_nodelay(config.tcp_nodelay)
        .connect()
        .await?;

    Ok(ret)
}

pub struct RichatSubscriber;

impl RichatSubscriber {
    pub async fn spawn_from_config(
        request: GrpcSubscribeRequest,
        config: GrpcConfig,
    ) -> Result<SubscribeStream, RichatSubscribeError> {
        let mut client = richat_client_from_config(config).await?;

        let stream = client.subscribe_richat(request).await?.into_parsed();

        Ok(stream)
    }
}
