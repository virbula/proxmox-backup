# Source File Analysis: `pipe_to_stream.rs`

## Overview
Implements utilities to bridge blocking I/O (like standard Unix pipes) with Async Streams.

## Functionality
* **`pipe_to_stream`**: Takes a synchronous `Write` interface (like a pipe from a subprocess) and converts it into an asynchronous `Stream` of data chunks.
* **Use Case**: Used when piping data *into* the backup client (e.g., `mysqldump | proxmox-backup-client backup dump.sql:-`).

## Dependencies
* `tokio::io`: Async I/O traits.