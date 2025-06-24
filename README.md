# Yellowstone Thorofare gRPC Benchmark Tool

A simple tool to compare the performance of two Yellowstone gRPC endpoints.

## Compilation

### Build Release Binary
```bash
cargo build --release
# Binary will be at: target/release/thorofare
```

### Run Directly with Cargo
```bash
cargo run --bin thorofare -- --endpoint1 https://endpoint1.com:443 --endpoint2 https://endpoint2.com:443
```

## Basic Usage

```bash
# Compare two endpoints
./thorofare --endpoint1 https://endpoint1.com:443 --endpoint2 https://endpoint2.com:443

# With authentication tokens
./thorofare \
  --endpoint1 https://endpoint1.com:443 \
  --endpoint2 https://endpoint2.com:443 \
  --x-token1 YOUR_TOKEN_1 \
  --x-token2 YOUR_TOKEN_2

# Collect more slots (default is 1000)
./thorofare \
  --endpoint1 https://endpoint1.com:443 \
  --endpoint2 https://endpoint2.com:443 \
  --slots 5000

# With load testing (subscribes to additional account updates)
./thorofare \
  --endpoint1 https://endpoint1.com:443 \
  --endpoint2 https://endpoint2.com:443 \
  --with-load \
  --config config-with-load-example.toml
```

## CLI Options

- `--endpoint1` - First gRPC endpoint URL
- `--endpoint2` - Second gRPC endpoint URL  
- `--x-token1` - X token for endpoint1 (optional)
- `--x-token2` - X token for endpoint2 (optional)
- `--slots` - Number of slots to collect (default: 1000)
- `--config` - Config file path (default: config.toml)
- `--output` - Output JSON file (default: benchmark_results.json)
- `--log-level` - Log level: debug/info/warn/error (default: info)
- `--with-load` - Enable load testing mode

## Config Files

- Use `config-example.toml` for normal benchmarks
- Use `config-with-load-example.toml` when using `--with-load`

## Output

Results are saved to `benchmark_results.json` (or your specified output file) with performance metrics like:
- Latency percentiles (p50, p90, p99)
- Slot processing times
- Network ping times
- Detailed timing breakdowns
