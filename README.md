# Leclerc

An analyzer of performance differences between a baseline and a mutant using Elastic APM traces.

## Overview

Leclerc is a tool for analyzing performance differences between two versions of a service (baseline and mutant) using traces from Elasticsearch APM. It uses the Mann-Whitney U test to determine if there are statistically significant differences in performance.

## Features

- Import traces from Elasticsearch APM
- Store and analyze trace data using Kuzu graph database
- Calculate rank biserial correlation to measure performance differences
- Generate reports of performance differences

## Requirements

- Python 3.11
- uv package manager
- Elasticsearch 8.x
- Kuzu database

## Usage

```bash
leclerc [OPTIONS] --span-name <SPAN_NAME> --baseline-start <BASELINE_START> --mutant-start <MUTANT_START>
```

### Options

- `-e, --elastic <ELASTIC>`: URL of the Elasticsearch instance containing traces for both the baseline and the mutant
- `-b, --baseline-elastic <BASELINE_ELASTIC>`: URL of the Elasticsearch instance containing traces for the baseline
- `-m, --mutant-elastic <MUTANT_ELASTIC>`: URL of the Elasticsearch instance containing traces for the mutant
- `-d, --database <DATABASE>`: Path to the Kuzu database for already ingested data
- `-n, --service-name <SERVICE_NAME>`: Name of the service to filter spans for
- `-s, --span-name <SPAN_NAME>`: Name of the span to be tested for performance differences
- `--duration <DURATION>`: Duration of trace recording (e.g., "15m")
- `--baseline-start <BASELINE_START>`: ISO 8601 date time of the start of baseline trace recording
- `--baseline-end <BASELINE_END>`: ISO 8601 date time of the end of baseline trace recording
- `--baseline-duration <BASELINE_DURATION>`: Duration of the baseline trace recording (e.g., "15m")
- `--mutant-start <MUTANT_START>`: ISO 8601 date time of the start of mutant trace recording
- `--mutant-end <MUTANT_END>`: ISO 8601 date time of the end of mutant trace recording
- `--mutant-duration <MUTANT_DURATION>`: Duration of the mutant trace recording (e.g., "15m")

### Examples

```bash
# Using a single Elasticsearch instance for both baseline and mutant
leclerc --elastic http://localhost:9200 \
        --span-name "http.request" \
        --service-name "my-service" \
        --baseline-start 2023-01-01T00:00:00Z \
        --baseline-duration "1h" \
        --mutant-start 2023-01-02T00:00:00Z \
        --mutant-duration "1h"

# Using separate Elasticsearch instances
leclerc --baseline-elastic http://baseline-es:9200 \
        --mutant-elastic http://mutant-es:9200 \
        --span-name "http.request" \
        --baseline-start 2023-01-01T00:00:00Z \
        --baseline-end 2023-01-01T01:00:00Z \
        --mutant-start 2023-01-02T00:00:00Z \
        --mutant-end 2023-01-02T01:00:00Z

# Using an existing Kuzu database
leclerc --database ./kuzu-db \
        --span-name "http.request" \
        --baseline-start 2023-01-01T00:00:00Z \
        --baseline-end 2023-01-01T01:00:00Z \
        --mutant-start 2023-01-02T00:00:00Z \
        --mutant-end 2023-01-02T01:00:00Z
```

## Libraries Used

- scipy: Mann-Whitney U-test to compute rank biserial correlation
- kuzu: Kuzu database binding
- elasticsearch: Elasticsearch client (async)
