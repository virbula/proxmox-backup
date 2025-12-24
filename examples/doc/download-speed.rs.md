# File Analysis: `download-speed.rs`

## Purpose
Tests the network download speed from a local Proxmox Backup Server.

## Implementation
* **Connection**: connect to `localhost:8007` as `root@pam`.
* **Client**: Uses `BackupReader::start` to initiate a restore session.
* **Method**: Calls `client.speedtest()`.
* **Output**: Returns the download speed in MB/s.
