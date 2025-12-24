# File Analysis: `block_driver.rs`

## Purpose
This file establishes the abstraction layer for the file restore system. It defines the `BlockDriver` trait, which standardizes how the application interacts with different backend technologies to access backup data.

By using a trait, the `main.rs` logic remains agnostic to whether data is being accessed via a local loopback mount, a QEMU VM, or potentially a network block device in the future.

## Key Components

### `trait BlockDriver`
This is the core interface that any backend must implement.

* **`list(images: Vec<String>, path: String) -> Result<Vec<ArchiveEntry>>`**
    * **Goal**: List files and directories within a specific path of the backup image.
    * **Input**: A list of image names (e.g., `drive-scsi0.img`) and the internal path to list.
    * **Output**: A vector of `ArchiveEntry`, which describes file metadata (name, size, type).

* **`data(images: Vec<String>, path: String, offset: u64, size: u64) -> Result<Box<dyn Read>>`**
    * **Goal**: Read raw data from a specific file.
    * **Input**: The image list, the file path, and byte ranges.
    * **Output**: A generic `Read` object containing the file content.

* **`status() -> Result<DriverStatus>`**
    * **Goal**: Check the health or readiness of the driver.

### `struct DriverMap` (Implied)
While the snippets don't fully show it, this module typically contains a registry or factory method to select the correct driver based on configuration or environment (e.g., defaulting to the QEMU driver).
