mod collector;
mod config;
mod grpc;
mod types;
mod processor;

pub use {
    config::Config,
    grpc::{GrpcClient, GrpcConfig},
    types::{EndpointData, SlotStatus},
    collector::Collector,
    processor::Processor,
};
