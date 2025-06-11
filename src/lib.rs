mod config;
mod grpc;
mod types;

pub use {
    grpc::{GrpcClient, GrpcConfig},
    types::SlotStatus,
};
