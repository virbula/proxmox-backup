# File Analysis: `crypt_config.rs`

## Purpose
Handles the high-level logic for encrypting and decrypting backup chunks. This is the core of the client-side encryption implementation.

## Key Structures

### `CryptConfig`
* **`cipher`**: The OpenSSL cipher in use (typically `aes-256-gcm`).
* **`key`**: The 32-byte master encryption key.

## Key Methods

* **`new(key: [u8; 32])`**: Initializes the config with a raw key.
* **`compute_digest(data: &[u8])`**: Calculates the checksum (fingerprint) of data.
* **`encrypt_to_writer`**: Takes a data stream, encrypts it using AES-256-GCM, and writes the output (including authentication tag) to a writer.
* **`decrypt_reader`**: Reads an encrypted stream, authenticates it, decrypts it, and produces the plaintext.
