# File Analysis: `prune.rs`

## Purpose
Configures Prune Jobs, which delete old snapshots based on retention policies. Stored in `prune.cfg`.

## Retention Policies
Implements the standard Proxmox retention algorithm:
* `keep-last`
* `keep-hourly`
* `keep-daily`
* `keep-weekly`
* `keep-monthly`
* `keep-yearly`
