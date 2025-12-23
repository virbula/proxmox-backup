# Source File Analysis: `backup_reader.rs`

## Overview
This module implements the `BackupReader` struct, which is responsible for reading data from a Proxmox Backup Server. It handles the logic for connecting to the server, verifying encryption configuration, and initiating downloads of chunks or files. It acts as a high-level client for **restore** operations.

## Key Structures

### `BackupReader`
The core struct that manages the state of a read session.
* **Fields**:
    * `h2`: An H2 client for HTTP/2 communication.
    * `abort`: A handle to abort async operations.
    * `crypt_config`: Optional configuration for encryption/decryption.

## Core Functionality

### 1. Initialization (`start`)
* Establishes a connection to the Backup Server.
* Performs a handshake to verify the protocol version.
* Downloads the `index.json` (backup manifest) to understand the structure of the backup (list of blobs, archives, and dynamic/fixed indexes).
* Verifies if the backup is encrypted and if the provided key matches the backup's key hash.

### 2. Downloading Data
* **`download`**: Generic method to download a blob (file) by name.
* **`download_chunk`**: Downloads a specific chunk of data referenced by its digest (hash). This is crucial for retrieving parts of large archives.
* **`speedtest`**: A utility function to test download speeds by requesting random chunks.

### 3. Index Handling
* The reader helps instantiate `FixedIndexReader` or `DynamicIndexReader` by fetching the index files (`.fidx` or `.didx`) from the server. These indexes map the backup content to specific chunks.

## Dependencies
* `pbs_datastore`: For handling manifests and indexes.
* `pbs_tools`: For cryptographic functions (SHA256) and configuration.
* `super::{H2Client, HttpClient}`: Relies on the internal HTTP client implementation.

## Security
* Validates that the encryption key fingerprint matches the one stored in the backup manifest (`key-fingerprint`) to prevent using the wrong key for decryption.