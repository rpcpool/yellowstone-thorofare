# Yellowstone Thorofare

Benchmarking tool for Solana Geyser gRPC endpoints. Compares two endpoints simultaneously to measure actual performance differences.

## Build
```bash
cargo build --release
```

## What it measures

- All 6 slot stages: FirstShredReceived, CreatedBank, Completed, Processed, Confirmed, Finalized
- Download time (FirstShredReceived > Completed)
- Replay time (CreatedBank > Processed)
- Account update propagation delays (optional)

## Basic usage
```bash
thorofare --endpoint1 endpoint1.com:10000 --endpoint2 endpoint2.com:10000
```

## Arguments

Required:
- `--endpoint1` - First endpoint (host:port)
- `--endpoint2` - Second endpoint (host:port)

Optional:
- `--x-token1` - Auth token for endpoint1
- `--x-token2` - Auth token for endpoint2
- `--slots` - Number of slots to collect (default: 1000)
- `--with-accounts` - Track account updates
- `--account-owner` - Filter by program ID (requires --with-accounts) (default: Raydium AMM V4)
- `--config` - Config file (default: config.toml)
- `--output` - Output file (default: benchmark_results.json)

## Examples

With auth tokens:
```bash
thorofare \
  --endpoint1 endpoint1.com:10000 \
  --endpoint2 endpoint2.com:10000 \
  --x-token1 YOUR_TOKEN_1 \
  --x-token2 YOUR_TOKEN_2
```

Track Raydium account updates:
```bash
thorofare \
  --endpoint1 endpoint1.com:10000 \
  --endpoint2 endpoint2.com:10000 \
  --with-accounts \
  --account-owner 675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8
```

## Output

Generates JSON with:
- Slot timing comparisons
- P50/P90/P99 percentiles
- Account update delays (if enabled)
- Ping latencies

You can visualize here https://thorofare.triton.one/

## Config

See config.toml for gRPC tuning options. Most important:
- `tcp_nodelay = true` for low latency
- `initial_connection_window_size` for heavy subscriptions
- `initial_connection_window_size` for heavy subscriptions
(Recommend reading: https://httpwg.org/specs/rfc9113.html#rfc.section.6.9.1, for window size)

## Contributing

PRs welcome. Keep it simple:
1. Fork and create a branch
2. Make your changes
3. Test against at least 2 different endpoints
4. Submit PR :D

## Related

- UI Repo: https://github.com/rpcpool/yellowstone-thorofare-ui
