# Source File Analysis: `vsock_client.rs`

## Overview
This module provides a client implementation for communicating over `AF_VSOCK`. This is specifically used when the PBS client is running inside a virtual machine (e.g., QEMU/KVM) and needs to talk to a backup agent or server running on the hypervisor (host) without using TCP/IP networking.

## Key Structures
* `VsockClient`: Similar to `HttpClient` but uses a VSOCK address (CID + Port).

## Use Case
* **PVE Integration**: Proxmox VE uses VSOCK to communicate securely between the host and the backup client inside the VM for file-level backups or restores, bypassing the need for network configuration inside the VM.

## Dependencies
* `nix` or `libc`: For socket syscalls related to VSOCK.
* `tokio`: For async socket handling.