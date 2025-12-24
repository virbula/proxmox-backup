# File Analysis: `proxmox_restore_daemon/watchdog.rs`

## Purpose
Implements a "dead man's switch" to prevent resource leaks.

## Problem
If the host process (the CLI tool) crashes or is killed *before* it can send a "stop" command to the VM, the VM would normally stay running indefinitely, consuming RAM and CPU.

## Solution

### The Watchdog Loop
* A background async task runs continuously (`watchdog_loop`).
* It maintains a `TRIGGERED` timestamp.
* If the time since the last "ping" exceeds `TIMEOUT` (e.g., 600 seconds), the watchdog initiates a system shutdown (`reboot(RB_POWER_OFF)`).

### Integration
* **Ping**: Every time the API receives a request, it calls `watchdog_ping()`, resetting the timer.
* **Inhibit**: For long-running operations (like extracting a large file that takes hours), the code can acquire a `WatchdogInhibitor`. This pauses the countdown until the operation finishes.
