# Source File Analysis: `api2/backup/upload_chunk.rs`

## Overview
CRITICAL FILE. This handler accepts individual data chunks uploaded by the client.

## Logic
1. Receives chunk data and a digest.
2. Verifies the digest matches the data.
3. Checks if the chunk already exists (deduplication).
4. If not, writes it to the chunk store.
5. Returns the size and status to the client.