# File Analysis: `read_chunk.rs`

## Overview
**Role:** Data Retrieval Interface

`read_chunk.rs` defines the **Traits** (interfaces) that decouple the *consumption* of data from the *source* of data. This allows the system to be modular: a restore process doesn't care if data comes from a local NVMe drive, a slow HDD, or an S3 bucket.

## Core Traits

### 1. `AsyncReadChunk`
This is the primary trait for async data access.
* **`read_raw_chunk(digest)`**: Returns the exact bytes stored on the backend (potentially encrypted/compressed).
* **`read_chunk(digest)`**: Returns the *payload* (decrypted and decompressed).

### 2. `ReadChunk` (Blocking)
A synchronous version of the trait, primarily used for legacy code paths or specific tools (like `proxmox-backup-debug`) that do not require an async runtime.

## Architectural Significance
By implementing this trait, developers can create new backends (e.g., an Azure Blob Storage backend) without rewriting the Restore logic, Catalog search, or FUSE mount implementations. They simply implement `AsyncReadChunk`, and the rest of PBS works automatically.

---
*Analysis generated based on source code.*