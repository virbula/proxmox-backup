# Source File Analysis: `backup_writer.rs`

## Overview
This module implements the `BackupWriter`, the counterpart to `BackupReader`. It handles the **backup** process, managing the upload of data chunks, indexes, and the final manifest to the server. It encapsulates the logic for "incremental" backups by only uploading chunks that do not already exist on the server.

## Key Structures

### `BackupWriter`
* **Fields**:
    * `h2`: HTTP/2 client.
    * `crypt_config`: Encryption configuration.
    * `verbose`: Flag for logging verbosity.
    * `status`: Tracks the upload status (dirty/clean).

## Core Functionality

### 1. Backup Session Management
* **`start`**: Initiates a backup session on the server via an API call.
* **`cancel`**: Cancels the current backup session.
* **`finish`**: Finalizes the backup, signing the manifest and committing the snapshot on the server.

### 2. Chunk Upload & Deduplication
* **`upload_chunk`**: The most critical method. It attempts to upload a chunk.
    * **Logic**: It likely checks if the server already has the chunk (via a "known chunks" cache or server response 200 OK vs 201 Created).
    * If encrypted, the chunk is encrypted locally before upload.
* **`upload_blob`**: Helper for uploading small configuration files (blobs) directly.

### 3. Index Management
* **`start_chunk_stream`**: Prepares the writer to accept a stream of chunks for a specific archive (e.g., `root.pxar`).
* It maintains the state of which chunks belong to which index (Fixed vs. Dynamic) and uploads the index files at the end of the stream.

## Notable Logic
* **Reusing Chunks**: It heavily integrates with the logic to detect if a chunk is already present on the server (Deduplication). It assumes the server calculates the hash or relies on the client sending the hash.
* **Encryption**: Ensures all chunks are encrypted (if config is present) before leaving the client.

## Dependencies
* `pbs_client::pxar`: For handling archive streams.
* `pbs_datastore`: For `BackupManifest` and index types.