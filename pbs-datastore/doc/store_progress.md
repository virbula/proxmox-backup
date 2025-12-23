# File Analysis: `store_progress.rs`

## Overview
**Role:** Progress Tracking Helper

`store_progress.rs` provides a thread-safe way to track the progress of long-running storage operations.

## Core Struct: `StoreProgress`
* **Fields:**
    * `done_chunks`: Number of chunks processed.
    * `total_chunks`: Total chunks expected.
    * `done_bytes`: Bytes processed.
    * `total_bytes`: Total bytes expected.

## Logic
It is often wrapped in an `Arc<Mutex<>>`. Worker threads (like GC workers) update the `done` counters atomically. A separate "Status" thread reads these counters to calculate the percentage ( `done / total * 100` ) and updates the task status shown in the Web UI.

---
*Analysis generated based on source code.*