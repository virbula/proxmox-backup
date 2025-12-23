# File Analysis: `lib.rs`

## Overview
**Role:** Module Entry Point & Public Interface

`lib.rs` is the root of the `proxmox-backup` storage library. In Rust, this file defines the module structure and determines which parts of the library are accessible to external crates (like the API server or the CLI client).

While it contains minimal logic itself, it is architecturally significant because it acts as the **Gatekeeper**, organizing the 20+ internal files into a coherent public API.

## Module Structure

The file organizes the library into logical subsystems:

### 1. Storage & Organization
* **`datastore`**, **`backup_info`**: Exposed to allow high-level management of backup groups and snapshots.
* **`chunk_store`**: Exposed for low-level interaction with the `.chunks` directory.

### 2. Indexes & Metadata
* **`manifest`**: Exposed to allow reading `index.json`.
* **`catalog`**: Exposed to allow file browsing.
* **`index`**, **`fixed_index`**, **`dynamic_index`**: Exposed to allow reading backup content (restores).

### 3. Data Processing
* **`read_chunk`**, **`local_chunk_reader`**: Exposed so restore jobs can fetch data.
* **`data_blob`**: Exposed for handling compression/encryption headers.

## Key Exports
The file typically re-exports commonly used structs to the top level for convenience. For example, it might allow a user to import `use pbs_datastore::DataStore;` instead of `use pbs_datastore::datastore::DataStore;`.

---
*Analysis generated based on the source code provided.*