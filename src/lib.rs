mod collector;
mod config;
mod grpc;
mod types;

pub use {
    config::Config,
    grpc::{GrpcClient, GrpcConfig},
    types::{EndpointData, SlotStatus},
};
