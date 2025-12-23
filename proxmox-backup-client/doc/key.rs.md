# Proxmox Backup Client - key.rs Analysis

## Overview
This module handles **Client-Side Encryption**. It manages the creation, import, and storage of encryption keys. In the PBS model, the server does not know the encryption key; it is managed entirely by the client.

## Commands

### `create`
- **Purpose**: Generates a new encryption key.
- **Logic**:
    - Generates 32 bytes of random data (AES-256 key).
    - Asks the user for a password.
    - Encrypts the key using the password and a KDF (Key Derivation Function: `Scrypt` or `PBKDF2`).
    - Saves the result to a `.key` file (default: `~/.config/proxmox-backup/encryption-key.json`).

### `import_with_master_key`
- **Purpose**: Recovers a backup key using a **Master Public Key**.
- **Scenario**: An admin creates a Master Key pair. The public key is used to encrypt the backup key into the backup manifest. If the user loses their password, the admin can use the private master key to decrypt the backup key and restore access.

### `change_passphrase`
- **Purpose**: Changes the password used to protect the local key file.
- **Note**: This does *not* change the underlying encryption key (which would make existing backups unreadable). It only re-wraps the key file with a new password.

### `paperkey`
- **Purpose**: Exports the key in a printable format (often with QR code generation logic handled by helper crates) for offline cold storage.

## Dependencies
- `pbs_key_config`: For key file format definitions.
- `openssl`: For RSA (Master Key) operations.
