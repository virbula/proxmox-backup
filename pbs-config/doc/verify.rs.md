# File Analysis: `verify.rs`

## Purpose
Configures Verification Jobs, which periodically check the cryptographic integrity of backup snapshots. Stored in `verification.cfg`.

## Functionality
* **Scheduling**: Defines when verification runs.
* **Scope**: Can verify an entire datastore or ignore verified chunks to save time (`ignore-verified`).
* **Outdated**: Can prioritize verifying snapshots that haven't been checked in X days.
