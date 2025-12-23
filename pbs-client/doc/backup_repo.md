# Source File Analysis: `backup_repo.rs`

## Overview
This file defines the `BackupRepository` struct, which represents the location of a backup store. It parses and stores the URI-like string used to identify a backup destination.

## Key Structures

### `BackupRepository`
* **Fields**:
    * `user`: User ID (e.g., `root@pam`).
    * `host`: Hostname or IP of the server.
    * `store`: Name of the datastore on the server.
    * `port`: Port number (default 8007).

## Functionality
* **Parsing**: Parses strings like `user@realm@host:datastore` or `host:datastore` into the struct.
* **Display**: Formats the struct back into a canonical string representation.

## Dependencies
* `regex`: For parsing the connection string.