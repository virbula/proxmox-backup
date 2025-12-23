# Source File Analysis: `pxar/create.rs`

## Overview
This module implements the creation of PXAR archives from the local filesystem. It is used during backups to walk the directory tree and serialize files into the PXAR stream.

## Key Structures/Enums
* `Encoder`: (Likely imported from `pxar` crate) used to write the format.

## Core Logic

### `create_archive`
* Traverses the source directory recursively.
* **Filtering**: Implements logic to exclude files based on patterns (`.pxarexclude` files or CLI flags).
* **Mount Points**: Detects mount points. By default, it might stop at mount boundaries (one filesystem per archive) unless configured otherwise.

### `add_entry`
* Reads file metadata (stat).
* Encodes the header (name, type, size, ownership).
* Reads file content and feeds it to the encoder.
* Handles special files (sockets, pipes, devices) by storing their metadata without content.

## Dependencies
* `walkdir` or internal recursion logic: To traverse directories.
* `pxar`: To encode the data structure.
* `pbs_client::pxar::metadata`: To extract metadata from the FS in a format compatible with PXAR.