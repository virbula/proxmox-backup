# File Analysis: `sha.rs`

## Purpose
Provides simple wrappers around OpenSSL's SHA functionality.

## Functions
* **`sha256(data: &[u8]) -> [u8; 32]`**: Computes the SHA-256 hash of a byte slice.
* **`sha256_reader(reader: &mut dyn Read) -> Result<([u8; 32], u64)>`**: Computes the hash of a data stream, also returning the total number of bytes hashed.
