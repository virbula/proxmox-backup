# File Analysis: `tape-write-benchmark.rs`

## Purpose
Benchmarks the write performance of a tape drive.

## Functionality
* **Arguments**: Takes a drive configuration name (e.g., `linux-drive-1`) and a number of gigabytes to write.
* **Data Generation**: Creates a stream of pseudo-random data.
* **Writing**:
    * Opens the tape drive using `DriveConfig`.
    * Uses `TapeWriter` to write the data in blocks.
    * Commits the transaction (writes filemarks).
* **Metrics**: Prints the average write speed in MB/s.
