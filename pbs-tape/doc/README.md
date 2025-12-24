# Proxmox Backup Server Tape (`pbs-tape`) Analysis

This directory contains the analysis of the `pbs-tape` crate. This library provides the low-level and high-level interfaces for interacting with Magnetic Tape drives (LTO) and Media Changers (Robots) on Linux. It handles SCSI commands, data blocking for reliability, and hardware management.

## Core Responsibilities

1.  **SCSI Passthrough**: Communicates directly with tape hardware using `SG_IO` ioctls.
2.  **Data Blocking**: Wraps raw backup data in a robust "Blocked" format with magic numbers, sequence IDs, and size headers to handle the sequential nature of tape.
3.  **Hardware Management**: functionality to load/unload tapes, format media, and read drive alerts/statistics.
4.  **Device Enumeration**: Scans `/sys` to find attached tape devices and match them to configuration.

## Module Overview

### Data Format (Blocking)
* **`blocked_reader.rs`**: Reads the Proxmox Tape format. Validates headers and sequence numbers while stripping them to provide a clean data stream.
* **`blocked_writer.rs`**: Writes the Proxmox Tape format. Chunks data into blocks and adds headers for integrity.

### Hardware Drivers
* **`sg_tape.rs`**: The primary driver for LTO drives. Handles commands like `REWIND`, `SPACE`, `WRITE FILEMARKS`, and status checking.
* **`sg_pt_changer.rs`**: The driver for Media Changers. Handles `MOVE MEDIUM` to load tapes from slots into drives.
* **`tape_alert.rs`**: Decodes the 64 standard SCSI Tape Alert flags (warnings about hardware health or media quality).

### System Integration
* **`linux_list_drives.rs`**: Utilities to scan the Linux system for tape devices, gathering serial numbers and device paths.
* **`tape_device.rs`**: Defines the `TapeRead` and `TapeWrite` traits that abstract the underlying hardware (or emulator).

### Utilities
* **`lib.rs`**: The crate root, exporting modules and defining common types like `BlockHeader`.
