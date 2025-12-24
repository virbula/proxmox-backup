# File Analysis: `fuse_loop.rs`

## Purpose
Orchestrates the creation of a "Loopback-backed FUSE Session". It exposes a data stream as a FUSE file and maps it to a block device.

## Key Structures

### `FuseLoopSession`
The main handle for the operation.
* **`reader`**: The source of data (e.g., a `AsyncRead` + `AsyncSeek` trait object).
* **`fuse_path`**: The path to the temporary FUSE mount (e.g., `/run/pbs-loopdev/...`).
* **`loop_dev_path`**: The path to the allocated loop device (e.g., `/dev/loop0`).

## Key Functions

* **`map_loop`**:
    1.  Creates a temporary directory.
    2.  Initializes a `Fuse` session exposing the data.
    3.  Calls `loopdev::get_or_create_free_dev` to find a slot.
    4.  Calls `loopdev::assign` to link the FUSE file to the loop device.
* **`main`**: The event loop. It waits for the FUSE session to initialize and then enters a polling loop to handle FUSE requests.
* **`FuseRequest` Implementation**:
    * `read`: Forwards read requests from the kernel directly to the underlying `reader`.
    * `getattr`: Returns simple file metadata (Read-Only, fixed size).
    * `lookup`: Resolves the single file exposed by this filesystem.

## Behavior
The FUSE filesystem generated here is extremely minimal. It contains only the root directory and one file (the image).
