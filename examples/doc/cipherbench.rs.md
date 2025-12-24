# File Analysis: `cipherbench.rs`

## Purpose
A micro-benchmark tool to measure the performance of cryptographic primitives and compression algorithms on the host CPU.

## Algorithms Tested
1.  **CRC32**: Basic integrity checksum.
2.  **Zstd**: Compression (at level 1).
3.  **SHA256**: Cryptographic hashing (OpenSSL).
4.  **AES-256-GCM**: Authenticated encryption (OpenSSL).
5.  **Poly1305**: Message authentication code (independent test).

## Implementation
* **`rate_test`**: A helper function that runs a closure in a loop for at least 1 second.
* It calculates the throughput in MB/s by dividing the total bytes processed by the elapsed time.
* Uses `proxmox_sys::linux::random_data` to generate a 1MB test buffer.
