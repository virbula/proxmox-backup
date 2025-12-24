# File Analysis: `drive.rs`

## Purpose
Configures physical Tape hardware. Stored in `drive.cfg`.

## Components
* **LTO Drive**: Configuration for a standalone tape drive (path, changer mapping).
* **Changer**: Configuration for a tape robot/library (SCSI path, drive slots).
* **Virtual**: Supports virtual tape drives for testing.
