# File Analysis: `main.rs`

## Purpose
The entry point for the restore daemon. It orchestrates the startup sequence, ensuring the environment is correct (running inside a restore VM) and launching the API server.

## Key Functionality

### 1. Environment Detection
* **`VM_DETECT_FILE`**: Checks for the existence of specific marker files (e.g., `/restore-vm-marker`) to confirm it is running in the correct environment.
* **`init_disk_state`**: Calls into the `disk` module to perform an initial scan of attached block devices.

### 2. Communication Setup (VSOCK)
Unlike standard web servers that listen on TCP ports, this daemon listens on `vsock`.
* **`DEFAULT_VSOCK_PORT`**: It binds to a standard port defined in the `pbs-client` crate.
* **`VsockListener`**: It uses a specialized listener to accept connections from the host hypervisor.

### 3. REST Server Loop
* It initializes a `RestServer` (from `proxmox-rest-server`).
* It registers the router defined in `api.rs`.
* It enters an async `tokio` loop to handle incoming API requests.
