# Source File Analysis: `tools/key_source.rs`

## Overview
This utility module handles the retrieval of encryption keys. It provides a unified interface to get the key material required for encryption/decryption from various sources.

## Key Functionality
* **Sources**:
    * **File**: Reads from a keyfile (default usually `~/.config/proxmox-backup/encryption-key.json`).
    * **Environment Variable**: Reads key from `PBS_ENCRYPTION_PASSWORD` or similar.
    * **Stdin**: Can read the key from standard input (useful for piping in scripts).
    * **Master Key**: Handles master key logic (RSA) to recover the actual encryption key.
* **Key derivation**: If the input is a password, it handles the KDF (Key Derivation Function) to generate the actual AES key.

## Dependencies
* `pbs_tools::crypt_config`: For key structures.