# File Analysis: `block_driver_qemu.rs`

## Purpose
This is the concrete implementation of the `BlockDriver` trait using QEMU. It manages the lifecycle of temporary "Restore VMs" that act as bridges between the host system and the backup snapshots.

## Key Structures

### `struct QemuBlockDriver`
The main struct implementing the `BlockDriver` trait.
* **Role**: It orchestrates the entire flow: ensuring a VM is running for the given backup, sending commands to it, and handling the responses.

### `struct VMState` & `VMStateMap`
* **`VMState`**: Holds runtime information about a specific Restore VM:
    * `pid`: Process ID of the QEMU instance.
    * `cid`: Context ID (CID) for the VSOCK communication channel.
    * `ticket`: Authentication ticket for the API.
* **`VMStateMap`**: Manages the persistence of `VMState` objects, typically serialized to a JSON file (`restore-vm-map.json`). This ensures that if the tool is run multiple times, it can reuse an existing VM instead of spinning up a new one every time (caching).

## Core Logic

### VSOCK Communication
The driver does not use TCP/IP networking to communicate with the VM, which would require complex bridging. Instead, it uses **Virtio-VSOCK**:
* The host connects to `/run/proxmox-backup/file-restore-serial-{cid}.sock`.
* It uses the `pbs_client::VsockClient` to send REST-like API requests to the agent running inside the VM.

### Lifecycle Management
1.  **Check Map**: When a request comes in, it checks `restore-vm-map.json` to see if a VM is already running for the requested snapshot.
2.  **Health Check**: It pings the existing VM.
3.  **Spawn (if needed)**: If no VM exists or the existing one is dead, it calls `qemu_helper::start_vm`.
4.  **Execute**: Once connected, it forwards the `list` or `extract` command to the agent inside the VM.
