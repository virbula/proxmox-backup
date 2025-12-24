# File Analysis: `proxmox_restore_daemon/api.rs`

## Purpose
This file defines the public interface (API) of the daemon. It maps HTTP-like requests received over VSOCK to specific Rust functions.

## Endpoints

* **`GET /api2/json/status`**
    * **Goal**: Health check.
    * **Returns**: System uptime, load average, and mount status.

* **`GET /api2/json/list`**
    * **Goal**: Browse the filesystem.
    * **Input**: A path (e.g., `/mnt/data/home/user`).
    * **Logic**: Delegates to `disk.rs` to list directory entries (files, folders) with their metadata (size, mtime, attributes).

* **`GET /api2/json/extract`**
    * **Goal**: Retrieve file content.
    * **Input**: A file path.
    * **Logic**: Opens the file on the mounted image and streams the bytes back to the caller.

* **`POST /api2/json/stop`**
    * **Goal**: Shutdown.
    * **Logic**: Triggers a clean shutdown of the VM.

## Routing
It uses the `proxmox_router` crate to define a `Router` tree, ensuring strict type checking and schema validation for all inputs.
