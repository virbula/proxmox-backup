# Proxmox Backup Server Key Configuration (`pbs-key-config`)

This library manages the secure storage, retrieval, and derivation of client-side encryption keys for Proxmox Backup Server. It defines the on-disk format for key files (typically `.key` files) and handles the cryptographic operations required to lock and unlock these keys using a user-provided passphrase.

## Overview

In the Proxmox Backup ecosystem, backups can be encrypted on the client side before being sent to the server. To do this, the client generates a random 256-bit master key. This library is responsible for:

1.  **Encrypting** that master key with a user's password (using a Key Derivation Function).
2.  **Storing** the encrypted key metadata (salt, KDF parameters, modification times) to disk.
3.  **Loading & Decrypting** the key when needed for backup or restore operations.
4.  **Verifying** integrity using fingerprints.

## Core Data Structures

### `KeyDerivationConfig`
An enum representing the algorithm and parameters used to derive a cryptographic key from a human-readable passphrase.

* **`Scrypt`** (Default/Preferred): Uses the memory-hard Scrypt algorithm. Stores parameters `n`, `r`, `p`, and a random `salt`.
* **`PBKDF2`**: Uses the standard PBKDF2 algorithm. Stores iteration count `iter` and `salt`.

### `KeyConfig`
*Note: While often defined alongside API types, this structure is the core focus of this library's logic.*

Represents the JSON structure stored in a `*.key` file.
* **`kdf`**: The `KeyDerivationConfig` used to lock this key.
* **`data`**: The actual 32-byte master key, encrypted using the derived password key.
* **`fingerprint`**: A SHA-256 checksum of the *unencrypted* master key. This is used to verify that decryption was successful and the key is correct.
* **`created` / `modified`**: Timestamps for key lifecycle management.
* **`hint`**: An optional password hint.

## Key Functions

### Key Derivation
* **`KeyDerivationConfig::derive_key(&self, passphrase: &[u8])`**
    * Takes a user passphrase.
    * Applies the configured KDF (Scrypt or PBKDF2) using the stored salt and parameters.
    * Returns a 32-byte key used to unwrap the actual master key.

### Encryption & Storage
* **`encrypt_key(key: &[u8], passphrase: &[u8], hint: Option<String>) -> KeyConfig`**
    * Generates a new random salt.
    * Derives a wrapper key from the passphrase using Scrypt.
    * Encrypts the provided raw `key` using the wrapper key.
    * Calculates the fingerprint of the raw key.
    * Returns the populated `KeyConfig` struct.
* **`store_key(path: &Path, key: &KeyConfig) -> Result<(), Error>`**
    * Serializes the `KeyConfig` to JSON.
    * Writes it to the specified `path` atomically using `replace_file`.
    * **Security**: Sets file permissions to `0o600` (read/write only by owner).

### Decryption & Loading
* **`decrypt_key(data: &[u8], passphrase_cb: Fn() -> Result<Vec<u8>, Error>) -> Result<Vec<u8>, Error>`**
    * Parses the JSON data into a `KeyConfig`.
    * Extracts the KDF parameters.
    * Prompts for the password using the callback.
    * Derives the wrapper key.
    * Decrypts the `data` field.
    * **Verification**: Computes the fingerprint of the decrypted result and compares it against `KeyConfig.fingerprint`. If they mismatch, it rejects the key (preventing silent corruption or incorrect password usage).
* **`load_and_decrypt_key(path: &Path, passphrase_cb: ...)`**
    * Helper wrapper that reads the file from disk and calls `decrypt_key`.

## Security Features

1.  **Salted Keys**: Every key generation uses a unique random salt, preventing rainbow table attacks against weak passwords.
2.  **Fingerprint Validation**: The library explicitly verifies the integrity of the key after decryption. This distinguishes between "wrong password" and "corrupted data" scenarios.
3.  **Atomic Writes**: Key files are written to a temporary location and renamed, ensuring the config file is never in a half-written state.
4.  **Restrictive Permissions**: Files are strictly locked to the owner (`0600`) upon creation.