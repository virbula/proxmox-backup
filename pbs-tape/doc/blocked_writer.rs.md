# File Analysis: `blocked_writer.rs`

## Purpose
Implements the writer for the "Blocked" tape format. It takes a stream of data and chunks it into blocks suitable for writing to a tape drive.

## Key Features

* **Chunking**: Buffers incoming data until it fills a block (e.g., 256KB or similar configured size).
* **Header Generation**: Prepends a `BlockHeader` containing the Magic Number, Sequence Number, and Payload Size.
* **Padding**: If the data doesn't fill a block completely (e.g., at the end of a stream), it zeros out the remainder.
* **Flush**: Ensures all buffered data is committed to the tape.
