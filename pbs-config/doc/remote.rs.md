# File Analysis: `remote.rs`

## Purpose
Configures "Remotes" - other Proxmox Backup Server instances that this server can connect to. Config stored in `remote.cfg`.

## Key Config
* **`hostname`**: Address of the remote server.
* **`auth-id`**: User/Token to authenticate as.
* **`password`**: The credentials (stored locally).
* **`fingerprint`**: The SSL certificate fingerprint for verification (crucial for self-signed certs).
