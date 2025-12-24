# File Analysis: `sg_pt_changer.rs`

## Purpose
The driver for SCSI Media Changers (Robotic Libraries). Allows moving cartridges between storage slots, import/export mail slots, and tape drives.

## Key Functions

* **`status`**: Queries the changer to get a map of which slots contain which tapes (reading barcodes if available).
* **`move_medium`**: The core command to move a tape from `Source` to `Destination`.
* **`element_status`**: Reads the detailed status of specific elements (slots, drives, transport arms).
