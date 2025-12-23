# File Analysis: `prune.rs`

## Overview
**Role:** Retention Policy Engine

`prune.rs` implements the logic for **Garbage Collection of Metadata**. It answers the question: *"Which backup snapshots should I keep, and which should I delete?"*

It implements a "Time-Bucket" algorithm (Keep Daily, Keep Weekly, etc.) commonly used in backup solutions to reduce storage usage over time while preserving historical granularity.

## Core Abstractions

### 1. `PruneOptions`
* **Concept:** Configuration struct defining the rules.
* **Fields:**
    * `keep_last`: How many recent backups to keep (e.g., 5).
    * `keep_hourly`: How many past hours to cover.
    * `keep_daily`, `keep_weekly`, `keep_monthly`, `keep_yearly`: The standard retention tiers.

### 2. `PruneResult`
* **Concept:** A struct attached to every snapshot during the calculation.
* **Fields:**
    * `mark`: The decision (`Keep` or `Remove`).
    * `keep_reason`: A human-readable string explaining *why* it is kept (e.g., "keep-daily").

## The Prune Algorithm

The logic is non-trivial because intervals overlap (today is also part of this week and this month). The algorithm in `compute_prune_info` works as follows:

1.  **Sorting:** Takes a list of all backups for a group, sorted by time (newest first).
2.  **Bucketing:** It iterates through the list and attempts to fill "slots" for each rule.
    * *Example:* For `keep-daily=7`, it creates time slots for the last 7 days.
    * It places the **newest** backup that falls into a specific day's slot into that bucket.
3.  **Conflict Resolution:** A single backup can satisfy multiple rules. If a backup is kept because it's the "Weekly" backup, it is also counted as the "Monthly" backup if it fits that slot too.
4.  **Marking:**
    * If a backup fills a required slot, it is marked `Keep`.
    * If it falls into a slot that is already full (e.g., we already have a backup for that Tuesday), it is marked `Remove` (unless `keep_last` saves it).
5.  **Protection Override:** Crucially, even if the math says "Remove", the caller (`backup_info.rs`) will verify if the file is manually `.protected` before actually deleting it.

## Why is this file critical?

Without `prune.rs`, storage would fill up indefinitely. This file ensures:
1.  **Space Efficiency:** Old backups become sparse (1 per year) instead of dense.
2.  **Compliance:** Allows organizations to meet legal requirements (e.g., "Must keep 7 years of data").

---
*Analysis generated based on the source code provided.*