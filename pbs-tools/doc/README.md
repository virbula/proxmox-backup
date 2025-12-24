# Proxmox Backup Server Tools (`pbs-tools`) Analysis

This directory contains the analysis of the `pbs-tools` crate. This library serves as a toolbox of shared utilities used across the entire Proxmox Backup Server codebase (client, server, and libraries).

## Core Capabilities

1.  **Cryptography**: Wrappers for OpenSSL to handle Chunk encryption (`CryptConfig`), SHA256 hashing, and Certificate management.
2.  **Caching**: Efficient in-memory caching structures (`LruCache`) including an asynchronous wrapper designed for concurrent high-load environments.
3.  **Parsing**: Helper combinators for the `nom` parsing library, used for parsing config files and protocols.
4.  **Formatting**: Utilities to display data sizes, time durations, and other types in a human-readable format.

## Module Breakdown

### Caching
* **`lru_cache.rs`**: A standard, synchronous Least Recently Used cache implementation.
* **`async_lru_cache.rs`**: A thread-safe, async-aware wrapper around `LruCache`. It features request coalescing: if multiple tasks ask for the same missing key, only one fetch is performed, and the result is broadcast to all waiters.

### Cryptography
* **`crypt_config.rs`**: Defines the `CryptConfig` struct. This is central to the backup encryption, handling the AES-256-GCM encryption/decryption of data chunks using a master key.
* **`cert.rs`**: Utilities for X.509 certificates. Includes logic to generate self-signed certificates (used by the PBS API server by default) and manage PEM files.
* **`sha.rs`**: Helper functions to calculate SHA-256 checksums of byte slices or `Read` streams.

### Utilities
* **`format.rs`**: Formatting functions. Example: converting `1073741824` bytes to `"1.00 GiB"`, or formatting durations.
* **`json.rs`**: Helpers for `serde_json` values, often used for validating or extracting data from JSON API payloads.
* **`nom.rs`**: Common parsing routines using the `nom` library (e.g., parsing whitespace, identifiers) to avoid duplication in other parsers.
