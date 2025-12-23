# Source File Analysis: `backup_specification.rs`

## Overview
This module handles the parsing of the backup source specification passed via the command line. It defines *what* to backup.

## Key Structures

### `BackupSpecification`
* **Enum/Struct**: Defines the type of backup source.
    * `pxar`: A file-level archive of a directory (`root.pxar:/`).
    * `img`: A block-device image (`drive.img:/dev/sdX`).
    * `conf`: A configuration blob (`config.json:./conf`).

## Functionality
* **Parsing**: Parses arguments like `root.pxar:/` to split the archive name (`root.pxar`) from the local path (`/`).
* **Validation**: Ensures the extension matches the content type (pxar vs img).

## Dependencies
* `anyhow`: For error handling in parsing.