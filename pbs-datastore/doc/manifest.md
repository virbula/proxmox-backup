# File Analysis: `manifest.rs`

## Overview
**Role:** Snapshot Inventory & Integrity Controller

`manifest.rs` defines the structure and logic for the **Backup Manifest** (`index.json`). This file is the "Brain" of every snapshot. It lists exactly which files exist in the backup, how they are encrypted, and—crucially—contains the cryptographic signature proving the backup hasn't been tampered with.

## Core Abstractions

### 1. `BackupManifest`
* **Concept:** The Rust representation of `index.json`.
* **Content:**
    * `backup-type`/`id`/`time`: Metadata identifying the snapshot.
    * `files`: A list of `FileCInfo` objects (the actual data).
    * `signature`: An RSA/Ed25519 signature of the manifest content.
    * `unprotected`: A dictionary to store mutable notes/comments without breaking the signature.

### 2. `FileCInfo` (File Info)
Represents a single file inside the snapshot (e.g., `drive-scsi0.img.fidx`).
* **filename:** The name on disk.
* **crypt_mode:** How it is encrypted (None, Encrypt, SignOnly).
* **size:** Logical size of the restored file.
* **csum:** A BLAKE2b checksum of the index file itself (integrity check).

## Key Mechanisms

### 1. Cryptographic Signing
The most critical feature of this file is **Integrity**.
* **Generation:** When a backup finishes, the server signs the manifest text using its private key.
* **Verification:** `verify_signature()` allows any client (or the verify job) to check if the manifest was modified by an unauthorized party. If the signature doesn't match the content, the backup is flagged as compromised.

### 2. Mutable vs. Immutable Data
A backup snapshot is supposed to be immutable (read-only). However, users often want to add **Notes** or change **Verification Status** (e.g., "Verified OK") after the backup is made.
* **The Problem:** Changing a single byte in `index.json` would break the cryptographic signature.
* **The Solution:** The manifest struct separates "Signed Data" (the file list) from "Unprotected Data" (notes, verify state). The signature only covers the file list, allowing metadata updates without invalidating the cryptographic proof.

### 3. Consistency Checks
The `check_fingerprint` method ensures that the file names listed in the JSON match the actual index files loaded from disk, preventing "Ghost Files" (files listed but missing) or "Hidden Files" (files present but not listed).

---
*Analysis generated based on the source code provided.*