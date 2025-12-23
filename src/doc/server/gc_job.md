# Source File Analysis: `server/gc_job.rs`

## Overview
Implements the Garbage Collection (GC) logic for datastores.

## Core Logic
1. **Mark Phase**: Iterates through all index files (.fidx, .didx) of all snapshots. Marks chunks in the `.chunks` directory as 'used' (often by updating atime).
2. **Sweep Phase**: Scans the chunk store. Deletes chunks that were not marked in the previous phase and are older than the safety margin.
3. **Locking**: Acquires an exclusive lock on the datastore during the sweep phase.