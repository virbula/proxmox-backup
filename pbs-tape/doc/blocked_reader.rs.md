# File Analysis: `blocked_reader.rs`

## Purpose
Implements the reader for the custom "Blocked" tape format used by PBS. Tape drives prefer writing large, fixed-size blocks. PBS wraps the data stream in its own blocks to ensure integrity and handle stream boundaries.

## Key Features

* **Header Validation**: Checks for `PROXMOX_TAPE_BLOCK_HEADER_MAGIC` to ensure the tape was written by PBS.
* **Sequence Checking**: Verifies `seq_nr` in each block header to detect dropped blocks or positioning errors.
* **Unblocking**: Strips the headers and presents a continuous `Read` interface to the consumer.
* **EOD/EOF Handling**: Detects End of File marks and End of Data conditions.
