# Proxmox Backup Server Examples Analysis

This directory contains standalone example scripts, benchmarks, and test utilities found in the `examples/` directory of the Proxmox Backup Server source code.

## Categories

### Performance Benchmarking
* **`cipherbench.rs`**: Measures the raw throughput of CPU-intensive algorithms like AES-256-GCM, SHA256, and Zstandard compression.
* **`tape-write-benchmark.rs`**: Tests the sustained write speed of a connected LTO tape drive.
* **`upload-speed.rs`**: Connects to a local PBS instance and measures backup upload throughput.
* **`download-speed.rs`**: Connects to a local PBS instance and measures restore download throughput.

### Chunking & Deduplication Tests
* **`test_chunk_size.rs`**: Analyzes the distribution of chunk sizes produced by the chunker on random data.
* **`test_chunk_speed.rs` / `test_chunk_speed2.rs`**: Measures how fast the system can slice a data stream into chunks (pure CPU test).

### Utilities
* **`dynamic-files.rs`**: Generates synthetic test files with predictable changes to verify incremental backup logic.
* **`completion.rs`**: Generates shell auto-completion scripts.
