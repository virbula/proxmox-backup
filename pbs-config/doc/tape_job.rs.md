# File Analysis: `tape_job.rs`

## Purpose
Configures Tape Backup jobs. Stored in `tape-job.cfg`.

## Functionality
* **Backup**: Copies data from a disk Datastore to a Tape Media Pool.
* **Scope**: Can filter which datastore, namespace, or snapshots to back up.
* **Schedule**: When to run the tape backup.
* **Drive**: Which drive/changer to use.
