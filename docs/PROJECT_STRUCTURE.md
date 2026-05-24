# Project Structure

This document maps the current repository layout and the responsibility of each major file.

```text
.
├── src/
│   ├── main.rs              # CLI shell: logging, config parsing, exit codes
│   ├── lib.rs               # Public library module declarations
│   ├── config.rs            # CLI arguments and options
│   ├── core.rs              # Extractor/Sink traits, events, materialized engine, run reports
│   ├── pipeline.rs          # Pipeline registry and default Apple Health CSV ZIP pipeline
│   ├── transform.rs         # TransformPolicy trait for grouping, sorting, filtering
│   ├── record.rs            # RecordProjection trait used by sinks
│   ├── error.rs             # Centralized typed errors
│   ├── xml_utils.rs         # Generic streaming XML reader helpers
│   ├── apple_health/
│   │   ├── extractor.rs     # Apple Health XML/ZIP extractor and export.xml selection
│   │   ├── policy.rs        # Apple Health grouping and sorting policy
│   │   ├── types.rs         # Generic Apple Health XML element record
│   │   └── mod.rs
│   └── sinks/
│       ├── csv_zip.rs       # Parallel CSV ZIP sink with safe names and temp-file persistence
│       └── mod.rs
├── tests/
│   ├── fixtures/
│   │   └── sample_export.xml
│   ├── integration_tests.rs # CLI/default pipeline and failure-path tests
│   └── unit.rs              # Framework, policy, projection, and sink tests
├── benches/
│   ├── parse.rs
│   └── flamegraph.rs
├── .github/workflows/
│   └── rust.yml
├── Cargo.toml
├── Cargo.lock
├── README.md
├── AGENTS.md
└── docs/
    └── PROJECT_STRUCTURE.md
```

## Architecture Overview

`gpt-os` is a reusable ETL framework with Apple Health as the default pipeline. The command `gpt-os <INPUT_FILE> <OUTPUT_ZIP>` still resolves to Apple Health input and CSV ZIP output, but the executable path is now composed explicitly:

```text
CliShell -> PipelineRegistry -> PipelineSpec -> Extractor -> TransformPolicy -> MaterializedEngine -> RecordProjection -> CsvZipSink
```

## Component Responsibilities

- **CliShell** (`main.rs`, `config.rs`): Parses CLI options, initializes logging, resolves a pipeline, runs it, and reports metrics.
- **PipelineRegistry** (`pipeline.rs`): Resolves `apple-health -> csv-zip` as the default pipeline and validates source/sink names.
- **MaterializedEngine** (`core.rs`): Receives extraction events, groups records in memory using a transform policy, sorts groups in parallel, and sends grouped records to a sink.
- **ExtractorRuntime** (`core.rs`, `xml_utils.rs`): Streams XML events through a bounded synchronous parser channel bridged to the public Tokio receiver and propagates parse/runtime errors.
- **TransformPolicy** (`transform.rs`, `apple_health/policy.rs`): Owns grouping, sorting, and filtering rules that used to live on records.
- **RecordProjection** (`record.rs`, `apple_health/types.rs`): Gives sinks source-neutral field access.
- **CsvZipSink** (`sinks/csv_zip.rs`): Serializes and compresses grouped CSV entries in parallel, merges them into a ZIP archive, and persists a temporary file to the final output path.
- **AppleHealthSource** (`apple_health/extractor.rs`, `apple_health/types.rs`): Owns Apple Health ZIP member selection, XML parsing, and record construction.

## Current Memory Model

Extraction is streaming, but the current engine is intentionally materialized: it groups all records into memory before writing CSV output. A future streaming or spooling engine should be added as a separate engine path rather than silently changing the current contract.

## Extension Guide

To add another source:

1. Define a record type and implement `RecordProjection` if existing sinks should consume it.
2. Implement `Extractor<YourRecord>`.
3. Implement `TransformPolicy<YourRecord>`.
4. Register a pipeline in `PipelineRegistry`.
5. Add framework and CLI tests that prove the default Apple Health pipeline still works.
