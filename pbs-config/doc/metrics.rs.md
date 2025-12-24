# File Analysis: `metrics.rs`

## Purpose
Configures external metric servers. Stored in `metricserver.cfg`.

## Functionality
* **InfluxDB**: Allows sending PBS performance data (disk usage, job status, IO) to an InfluxDB (HTTP or UDP) instance for visualization in Grafana.
