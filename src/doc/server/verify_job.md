# Source File Analysis: `server/verify_job.rs`

## Overview
Implements the Verification Job logic.

## Logic
* **Iterate**: Walks through snapshots in a datastore.
* **Load**: Reads the fixed or dynamic indexes.
* **Check**: Decodes chunks and validates their SHA-256 checksums matches their ID.
* **Reporting**: Marks snapshots as 'verified' or 'corrupt' in the manifest.