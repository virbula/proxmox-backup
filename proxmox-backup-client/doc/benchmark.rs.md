# Proxmox Backup Client - benchmark.rs Analysis

## Overview
The `benchmark.rs` module implements the `benchmark` subcommand. It provides a suite of performance tests to evaluate both the local system's cryptographic capabilities and the network connection to the Proxmox Backup Server (PBS).

## Key Structures

### `Speed` Struct
A serializable structure used to format the output of the benchmark tests.
- `speed`: The measured throughput in Bytes/second.
- `top`: A reference value (based on an AMD Ryzen 7 2700X) for comparison.

## Core Functions

### `benchmark` (Main Entry)
- **Signature**: `pub async fn benchmark(param: Value, rpcenv: &mut dyn RpcEnvironment) -> Result<Value, Error>`
- **Logic**:
    1.  **Repository Check**: It parses the `repository` argument.
    2.  **Network Test**: If a repository is provided, it connects to the server and calls `test_upload_speed`.
    3.  **Local Crypto Test**: It calls `test_crypt_speed` to benchmark CPU-bound operations.
    4.  **Output**: Results are returned as a JSON `Value` or printed in a table, showing "TLS", "SHA256", "Compression", "Decompression", "AES256/GCM", and "Verify" speeds.

### `test_upload_speed`
- **Purpose**: Measures the raw TLS upload throughput to the backup server.
- **Implementation**:
    - Connects to the server using `BackupWriter::start`.
    - Sends a special "speedtest" command to the server.
    - Can optionally disable local caching (`no_cache` flag) to ensure the test measures true network/disk write speed rather than reading from a local chunk cache.

### `test_crypt_speed`
- **Purpose**: Micro-benchmarks for local processing power, essential for client-side encryption and deduplication.
- **Tests**:
    - **SHA256**: Measures hashing speed (critical for chunking).
    - **ZStd**: Measures compression (level 1) and decompression.
    - **AES256-GCM**: Uses `openssl` to measure encryption speed.
    - **Verify**: Simulates verifying a chunk (hashing + decryption).

## Dependencies
- `pbs_client::BackupWriter`: For server communication.
- `pbs_tools::crypt_config`: For encryption benchmarks.
- `openssl` & `zstd`: For low-level cryptographic and compression operations.
