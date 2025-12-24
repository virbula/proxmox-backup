# File Analysis: `upload-speed.rs`

## Purpose
Tests the network upload speed to a local Proxmox Backup Server.

## Implementation
* **Connection**: connect to `localhost:8007` as `root@pam`.
* **Client**: Uses `BackupWriter::start` to initiate a backup session.
* **Method**: Calls `client.upload_speedtest()`, which streams random data to the server and measures the throughput.
* **Output**: Returns the speed in MB/s.
