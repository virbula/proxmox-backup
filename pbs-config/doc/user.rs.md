# File Analysis: `user.rs`

## Purpose
Manages the `user.cfg` file, defining users who can log in to the system.

## Key Fields
* **`userid`**: Unique identifier (e.g., `user@pbs`).
* **`email`**: Contact email for notifications.
* **`enable`**: Boolean to disable accounts without deleting them.
* **`expire`**: Expiration date for the account.
* **`firstname` / `lastname`**: Metadata.

## Notes
Passwords are *not* stored here; they are stored in `/etc/shadow` (for PAM) or a separate shadow file for PBS internal users.
