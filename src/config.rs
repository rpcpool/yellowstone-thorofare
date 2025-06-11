use {
    serde::{Deserialize, Serialize},
    std::time::Duration,
};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    FileReadError(#[from] std::io::Error),

    #[error("Failed to parse TOML: {0}")]
    TomlParseError(#[from] toml::de::Error),
}

pub type Result<T> = std::result::Result<T, ConfigError>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub grpc: GrpcConfig,
    pub benchmark: BenchmarkConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrpcConfig {
    // Initial connection timeout
    #[serde(with = "humantime_serde")]
    pub connect_timeout: Duration,

    // Timeout for individual requests
    #[serde(with = "humantime_serde")]
    pub request_timeout: Duration,

    // Max incoming message size (slot updates are tiny ~32 bytes)
    pub max_message_size: usize,

    pub use_tls: bool,

    // Dynamically adjusts flow control window based on BDP (bandwidth-delay product)
    pub http2_adaptive_window: bool,

    // Sends HTTP2 PING frames to detect dead connections
    #[serde(with = "humantime_serde", default)]
    pub http2_keep_alive_interval: Option<Duration>,

    // How much data the server can send before waiting for ACK
    pub initial_connection_window_size: Option<u32>,

    // Per-stream buffer before backpressure
    pub initial_stream_window_size: Option<u32>,

    // Send packets immediately (important for ping latency)
    pub tcp_nodelay: bool,

    // OS-level connection health check
    #[serde(with = "humantime_serde", default)]
    pub tcp_keepalive: Option<Duration>,

    // tonic's internal channel buffer
    pub buffer_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BenchmarkConfig {
    pub buffer_percentage: f32,
    pub latency_samples: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            grpc: GrpcConfig {
                connect_timeout: Duration::from_secs(30),
                request_timeout: Duration::from_secs(30),
                max_message_size: 1024 * 1024, // 1MB plenty for slot updates
                use_tls: true,
                http2_adaptive_window: false, // Not needed for small messages
                http2_keep_alive_interval: Some(Duration::from_secs(30)),
                initial_connection_window_size: Some(65535), // HTTP2 default
                initial_stream_window_size: Some(65535),     // HTTP2 default
                tcp_nodelay: true,                           // Faster pings
                tcp_keepalive: Some(Duration::from_secs(60)),
                buffer_size: Some(64), // Small - we process fast
            },
            benchmark: BenchmarkConfig {
                buffer_percentage: 0.1,
                latency_samples: 20,
            },
        }
    }
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&content)?)
    }
}
