# Source File Analysis: `backup_stats.rs`

## Overview
A module for tracking and calculating statistics during a backup or restore session.

## Functionality
* **Metrics**:
    * Total bytes processed.
    * Bytes uploaded (new data).
    * Bytes reused (deduplicated data).
    * Duration.
    * Chunk counts.
* **Reporting**: Formats these statistics for the log output (e.g., "Transferred: 1.5 GB (50%), Reused: 1.5 GB").