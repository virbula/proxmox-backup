# File Analysis: `test_chunk_speed.rs`

## Purpose
Benchmarks the raw CPU performance of the rolling hash (Buzhash) chunking algorithm.

## Implementation
* **Setup**: Creates a 32MB buffer of random data.
* **Loop**: Repeatedly feeds this buffer into the `Chunker` iterator.
* **Metric**: Calculates throughput (MB/s) based on how quickly it can determine chunk boundaries.
