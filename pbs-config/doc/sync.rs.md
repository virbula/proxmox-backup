# File Analysis: `sync.rs`

## Purpose
Configures Sync Jobs, which pull backup data from a `Remote` to a local `Datastore`. Stored in `sync.cfg`.

## Key Config
* **`remote`**: Reference to a Remote in `remote.cfg`.
* **`remote-store`**: The source datastore on the remote.
* **`store`**: The local destination datastore.
* **`schedule`**: Cron-like schedule for the job.
* **`ns`**: (Optional) Specific namespace to sync.
