# File Analysis: `dynamic-files.rs`

## Purpose
Generates synthetic files that change over time in a predictable way. This is useful for testing incremental backups.

## Usage
* Can create a file with specific "seekable" patterns.
* Can update the file, changing only specific chunks while leaving others identical.
* Used to verify that the backup client correctly identifies unchanged chunks and only uploads the modified parts.
