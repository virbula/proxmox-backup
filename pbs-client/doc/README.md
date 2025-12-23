# Proxmox Backup Client (pbs-client) Source Analysis

The `pbs-client` library provides the core client-side functionality for the Proxmox Backup Server ecosystem. It handles network communication, authentication, encryption, backup creation (encoding), restore (decoding), and repository management.

Below is a detailed description of each source file and its responsibility within the library.

## Core Backup & Restore Modules

### `backup_reader.rs`
**Purpose**: Handles reading and downloading existing backups from the server.
- **Key Struct**: `BackupReader`
- **Functionality**:
  - Establishes a Proxmox Backup Protocol connection (HTTP/2 upgraded).
  - Downloads specific artifacts: `manifest` (`index.json`), `blobs`, `fixed_index` (`.fidx`), and `dynamic_index` (`.didx`).
  - Verifies file integrity using manifests and checksums.
  - Provides a `speedtest` method to benchmark download performance.
  - Supports downloading individual chunks via `download_chunk`.

### `backup_writer.rs`
**Purpose**: Manages the logic for creating new backups and uploading data.
- **Key Struct**: `BackupWriter`
- **Functionality**:
  - Initiates the backup protocol and handles the HTTP/2 upload pipeline.
  - **Uploads**: Manages the upload of blobs (config/logs) and data streams (archives/images).
  - **Incremental Backup**: Supports "known chunks" to skip uploading data that already exists on the server.
  - **Indexing**: Generates and uploads index files (fixed or dynamic) corresponding to the uploaded data.

### `backup_repo.rs`
**Purpose**: Defines and parses the location of a backup repository.
- **Key Struct**: `BackupRepository`
- **Functionality**:
  - Parses connection strings in the format `user@host:datastore`.
  - Manages default ports (8007), authentication IDs, and host resolution (including IPv6).

### `backup_specification.rs`
**Purpose**: Parses the user's instructions for *what* to back up.
- **Key Struct**: `BackupSpecification`
- **Functionality**:
  - Parses arguments like `archive_name.pxar:/source/path`.
  - Validates archive extensions (`.pxar`, `.img`, `.conf`, `.log`).
  - Defines detection modes (`Legacy`, `Data`, `Metadata`) for handling file changes.

### `backup_stats.rs`
**Purpose**: Tracks statistics during backup and restore operations.
- **Key Structs**: `BackupStats`, `UploadStats`, `UploadCounters`.
- **Functionality**:
  - Monitors total size, compressed size, chunk counts (new vs. reused), and operation duration.
  - Uses atomic counters to safely track metrics across asynchronous tasks.

## Connection & Networking

### `http_client.rs`
**Purpose**: The central HTTP/HTTPS client wrapper.
- **Key Structs**: `HttpClient`, `H2Client`.
- **Functionality**:
  - Wraps the `hyper` client to handle TLS (OpenSSL) and HTTP/2 multiplexing.
  - **Authentication**: Manages API tokens, cookies, and CSRF tokens.
  - **Security**: Implements custom server certificate fingerprint verification.

### `vsock_client.rs`
**Purpose**: specialized client for communication over virtio-vsock.
- **Functionality**:
  - Allows Virtual Machines (VMs) to communicate directly with a backup server agent running on the host without needing a network stack.
  - Implements similar request/response patterns to the standard HTTP client but over VSOCK channels.

### `remote_chunk_reader.rs`
**Purpose**: A high-level reader for accessing chunks from a remote store.
- **Key Struct**: `RemoteChunkReader`.
- **Functionality**:
  - Implements the `AsyncReadChunk` trait, allowing it to be used transparently by other readers.
  - Handles the downloading and optional decryption of chunks on demand.
  - Uses a local cache to prevent re-downloading recently accessed chunks.

## Proxmox Archive (PXAR) Support
These files reside in the `pxar/` subdirectory and handle the custom archive format used for file-level backups.

- **`pxar/mod.rs`**: The module entry point exposing PXAR functionality.
- **`pxar/create.rs`**: Logic for **creating** PXAR archives.
    - Walks directory trees recursively.
    - Encodes file entries, directory structures, and special files (symlinks, devices).
    - Captures metadata including ACLs (Access Control Lists), xattrs, and file flags.
- **`pxar/extract.rs`**: Logic for **extracting** PXAR archives.
    - Reads a PXAR stream and reconstructs the file system on disk.
    - Restores metadata, permissions, and special file types.
- **`pxar/metadata.rs`**: Handles the translation of system metadata (stat, ACLs, capabilities) into the PXAR format and vice versa.
- **`pxar/dir_stack.rs`**: A helper utility to manage the state of directory traversal (stack operations) during archive creation or extraction.
- **`pxar/look_ahead_cache.rs`**: Implements caching strategies to optimize random access or sequential reading of PXAR archives.
- **`pxar/flags.rs`**: Definitions of file system flags (e.g., `FS_NOATIME_FL`, `FS_IMMUTABLE_FL`) and their mapping to different file systems like ZFS and BTRFS.
- **`pxar/tools.rs`**: Shared utility functions for PXAR operations, such as formatting entry output or handling paths.

## Stream & Chunk Management

- **`chunk_stream.rs`**: Manages the stream of data chunks. It likely handles the logic of breaking a continuous data stream into the fixed or dynamic chunks required by the deduplication engine.
- **`inject_reused_chunks.rs`**: Optimization logic. It identifies chunks in a stream that match "known" chunks on the server and "injects" them into the upload stream as references rather than uploading the raw data again.
- **`merge_known_chunks.rs`**: Merges consecutive "known" chunks into larger logical ranges to reduce overhead when processing index files or reporting statistics.
- **`pipe_to_stream.rs`**: Utilities to pipe standard input (stdin) or file descriptors into a backup stream.
- **`pxar_backup_stream.rs`**: A specialized stream implementation for sending PXAR data, ensuring it aligns with the chunking requirements of the backup server.

## Utilities & Tools

- **`catalog_shell.rs`**: Likely provides an interactive shell or query interface to navigate the file catalog (the directory structure stored in `index.json`) without fully downloading the archive.
- **`task_log.rs`**: Helper functions for formatting and displaying task logs, progress bars, and status updates to the user.
- **`tools/mod.rs`**: General utility module.
    - **Credential Management**: Retrieves passwords/secrets from systemd credentials, environment variables (`PBS_PASSWORD`), or files.
    - **Schema**: Defines JSON schemas for validation (e.g., Repo URL format, chunk sizes).
- **`tools/key_source.rs`**: Abstraction for retrieving encryption keys from various sources (files, standard input, etc.).
- **`lib.rs`**: The root library file that exports the public modules and types for use by other crates (like the `proxmox-backup-client` binary).