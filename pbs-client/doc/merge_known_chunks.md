# Source File Analysis: `merge_known_chunks.rs`

## Overview
Related to `inject_reused_chunks.rs`, this module specifically handles the logic of merging the stream of new data with the knowledge of old data.

## Functionality
* **Comparison**: It compares the state of the current file/stream with the `FixedIndex` or `DynamicIndex` of the previous backup.
* **Optimization**: If a sequence of chunks matches the old index, it emits them as "Reused", effectively performing client-side deduplication based on local metadata before even talking to the server.

## Dependencies
* `futures`: For stream processing.