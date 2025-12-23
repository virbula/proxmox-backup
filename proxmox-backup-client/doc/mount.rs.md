# Proxmox Backup Client - mount.rs Analysis

## Overview
The `mount.rs` module implements the `mount` subcommand. It allows users to mount a remote backup snapshot (specifically `.pxar` file archives) as a read-only filesystem on their local machine.

## Command: `mount`

### Usage
`proxmox-backup-client mount <snapshot> <archive_name> <mount_point>`

### Workflow
1.  **Connect**: Authenticates with the Backup Server.
2.  **Manifest**: Downloads the `index.json` manifest for the requested snapshot.
3.  **Resolve Archive**: Looks up the specific archive (e.g., `root.pxar`) within the manifest.
4.  **Accessor**: Calls `helper::get_pxar_fuse_accessor` to create a random-access reader for the remote data.
5.  **FUSE Session**:
    - Uses `proxmox_fuse::Session` or `Mount` to attach to the kernel FUSE interface.
    - Maps filesystem operations (read, getattr, readdir) to the `Accessor`.
6.  **Loop**: The command blocks (hangs) while the mount is active. It unmounts when the user interrupts (Ctrl+C).

## Technical Details
- **Read-on-Demand**: Data is not downloaded upfront. Chunks are fetched from the server only when the user reads a specific file in the mount point.
- **Decoding**: The client handles the AES-GCM decryption of chunks on the fly as they are requested by the OS kernel.
