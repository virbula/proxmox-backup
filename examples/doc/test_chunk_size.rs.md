# File Analysis: `test_chunk_size.rs`

## Purpose
Validates the chunking algorithm's behavior by analyzing the size distribution of generated chunks.

## Functionality
* **Input**: Generates a massive stream of random data (seeded for reproducibility).
* **Chunker**: Runs the `Chunker` with standard PBS settings (4MB avg, 64KB min, 16MB max).
* **Analysis**:
    * Counts chunks.
    * Calculates total bytes.
    * determines the average chunk size.
    * Reports the distribution (e.g., "Chunk size is 4.123 MB").
