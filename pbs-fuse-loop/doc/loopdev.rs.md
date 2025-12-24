# File Analysis: `loopdev.rs`

## Purpose
Provides low-level interfaces to the Linux Loop Device subsystem via `ioctl`.

## Implementation Details

It defines the C-compatible structs required by the kernel, such as `LoopInfo64`.

### Key Functions

* **`get_or_create_free_dev() -> String`**:
    * Opens `/dev/loop-control`.
    * Uses `ioctl(LOOP_CTL_GET_FREE)` to ask the kernel for the next available loop index.
    * Returns the path (e.g., `/dev/loop3`).
* **`assign(loop_dev: Path, backing: Path)`**:
    * Opens the loop device and the backing file (the FUSE mount).
    * Uses `ioctl(LOOP_SET_FD)` to bind them.
    * **Configuration**:
        * `LO_FLAGS_READ_ONLY`: Ensures writes are rejected (safe for backups).
        * `LO_FLAGS_PARTSCAN`: Critical. Tells the kernel to scan the loop device for partition tables (MBR/GPT) and create device nodes (e.g., `/dev/loop0p1`) automatically.
