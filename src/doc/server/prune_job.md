# Source File Analysis: `server/prune_job.rs`

## Overview
Implements the Prune Job logic.

## Logic
* **Scheduling**: Runs based on a calendar schedule.
* **Simulator**: Calculates which snapshots to keep based on retention options (keep-last, keep-daily, etc.).
* **Execution**: Removes the backup directories (manifests and indexes) of expired snapshots. Actual data deletion happens during GC.