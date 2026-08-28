# AppleHealth2CSV

`AppleHealth2CSV` is a personal ETL tool and reusable Rust framework for transforming personal data into formats that are easy to inspect, research, and import into other workflows. The default pipeline processes Apple Health exports and writes grouped CSV files into a ZIP archive. I created it to make apple health exports more manageable to upload to chatgpt chats.

The command-line default is intentionally still Apple Health:

```bash
gpt-os <INPUT_FILE> <OUTPUT_ZIP>
```

Internally, the pipeline is now composed from source, transform policy, and sink components so future data sources can be added without making Apple Health the whole application architecture.

## Features

- Default Apple Health source for `export.xml` files or ZIP archives containing `export.xml`.
- Explicit pipeline registry with `apple-health` source and `csv-zip` sink defaults.
- Materialized grouped engine: extraction streams records, then the engine groups records in memory before writing output.
- Apple Health transform policy for grouping `Record` elements by `type`, grouping other elements by name, and sorting date-like records.
- Source-neutral record projection so sinks do not require Apple Health records to implement CSV-specific traits.
- Parallel CSV ZIP output with safe encoded entry names and temporary-file persistence before replacing the target archive.
- Strict parse errors by default, with optional tolerant mode that skips malformed records and reports skipped counts.

## Installation

```bash
cargo build --release
```

## Usage

```bash
gpt-os [OPTIONS] <INPUT_FILE> <OUTPUT_ZIP>
```

### Arguments

- `<INPUT_FILE>`: Apple Health `export.xml` or a ZIP archive containing an `export.xml` member.
- `<OUTPUT_ZIP>`: Destination ZIP archive containing grouped CSV files.

### Options

- `-v, --verbose`: Enable verbose logging.
- `--no-metrics`: Disable end-of-run metrics on stdout.
- `--source <SOURCE>`: Source adapter. Defaults to `apple-health`.
- `--sink <SINK>`: Sink adapter. Defaults to `csv-zip`.
- `--tolerant`: Skip malformed record-level parse failures and report skipped counts.
- `-h, --help`: Show usage information.

### Example

```bash
gpt-os -v export.zip my_health_data.zip
```

## Architecture

The runtime path is:

```text
CliShell -> PipelineRegistry -> PipelineSpec -> Extractor -> TransformPolicy -> MaterializedEngine -> RecordProjection -> CsvZipSink
```

Key modules:

- `src/pipeline.rs`: resolves the default Apple Health CSV ZIP pipeline.
- `src/core.rs`: framework traits, materialized engine, extraction events, runtime reports, and error policy.
- `src/transform.rs`: transform policy trait for grouping, sorting, and filtering.
- `src/record.rs`: source-neutral record projection trait used by sinks.
- `src/apple_health/`: Apple Health extraction, records, and transform policy.
- `src/sinks/csv_zip.rs`: parallel CSV ZIP sink with safe entry names and temporary-file persistence.

See [docs/PROJECT_STRUCTURE.md](docs/PROJECT_STRUCTURE.md) for the full file map.

## Testing

```bash
cargo fmt --check
cargo check --all-targets
cargo test --all-targets
cargo clippy --all-targets --all-features -- -D warnings
```

The tests cover the default CLI pipeline, XML-vs-ZIP input equivalence, malformed XML behavior, tolerant mode, safe ZIP entry names, source-neutral record projection, and a synthetic non-Apple framework pipeline.

## Benchmarking

The Criterion benchmark in `benches/flamegraph.rs` runs the CLI against a generated synthetic Apple Health-style XML export by default. This keeps benchmark inputs private-data-free while exercising a real-sized pipeline path.

```bash
$env:GPT_OS_BENCH_RECORDS = "100000"
cargo bench --bench flamegraph
```

To benchmark a private local Apple Health export, set `GPT_OS_BENCH_INPUT`:

```bash
$env:GPT_OS_BENCH_INPUT = "C:\path\to\export.zip"
cargo bench --bench flamegraph
```

Private Apple Health exports should stay outside source control.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE).
