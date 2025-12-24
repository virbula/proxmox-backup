# File Analysis: `qemu_helper.rs`

## Purpose
This module contains the low-level logic required to configure and spawn the QEMU process. It abstracts the complexity of the QEMU command-line interface.

## Key Functionality

### `start_vm` Function
This is the critical function that constructs the `Command` to launch QEMU. Its responsibilities include:

1.  **Kernel & Initrd**: Locating the kernel image and the dynamic initramfs (created via `cpio.rs`) and passing them to `-kernel` and `-initrd`.
2.  **Memory Management**: Setting RAM size (defined by `MAX_MEMORY_DIMM_SIZE` and base requirements).
3.  **Block Devices**: Mapping the backup snapshots (chunks) to QEMU block devices. This often involves setting up virtio-blk-pci devices backed by the restore images.
4.  **Networking**: Configuring the `vhost-vsock-pci` device with a unique Context ID (CID) to enable host-to-guest communication without IP networking.
5.  **KVM**: Ensuring hardware acceleration (`-enable-kvm`) is active for performance.

### Timeout & Polling
After spawning the process, the code tracks the PID and polls for the VSOCK socket to become active. It implements retry logic to ensure the VM has fully booted before returning control to the driver.

### Resource Cleanup
It may include logic to ensure temporary files or lock files are cleaned up if the VM fails to start.
