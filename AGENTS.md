# AGENTS.md

Repository-local instructions for working on `gpt-os`.

## Product Direction

- `gpt-os` is a reusable Rust ETL framework with Apple Health as the default source.
- The default CLI path must remain:

```bash
gpt-os <INPUT_FILE> <OUTPUT_ZIP>
```

- Keep Apple Health working as the first-class default while avoiding Apple-Health-only abstractions in the framework core.
- Do not add separate assistant-specific guidance files. Keep durable repository guidance in this file and project documentation in `README.md` or `docs/`.

## Architecture Invariants

The default runtime path is:

```text
CliShell -> PipelineRegistry -> PipelineSpec -> Extractor -> TransformPolicy -> MaterializedEngine -> RecordProjection -> CsvZipSink
```

- `src/core.rs` owns framework traits, extraction events, the materialized engine, run reports, and error policy.
- `src/pipeline.rs` resolves the default `apple-health -> csv-zip` pipeline.
- `src/transform.rs` owns source-neutral grouping, sorting, and filtering policy.
- `src/record.rs` owns source-neutral field projection for sinks.
- `src/apple_health/` owns Apple Health extraction, XML element records, and Apple Health transform policy.
- `src/sinks/csv_zip.rs` owns CSV ZIP output.

The current engine is intentionally materialized: extraction streams records, then the engine groups records in memory before loading them into a sink. Do not describe the whole pipeline as low-memory streaming unless a streaming or spooling engine is actually added.

## Performance Invariants

Performance is part of correctness for this project.

- XML parsing must keep the reader, batch parsing, and downstream consumption overlapped.
- The extractor may expose a Tokio receiver publicly, but the parser hot path may use a bounded synchronous channel internally for throughput.
- Batch XML parsing must be joined before extraction reports success; do not return success while parser work is still running.
- Transform-policy sorting should remain parallel across materialized groups.
- CSV ZIP output should serialize and compress grouped entries in parallel, then merge them into the final archive.
- Do not replace the parallel parser or sink topology with a simpler serial implementation without benchmarking real-sized or generated large inputs.
- Keep safe ZIP entry names and temporary-file persistence for output archives.
- Avoid explicit durability calls such as `sync_all` on the hot path unless the requirement is deliberate and benchmarked.

## Error Handling And Safety

- Parse callbacks return `Result<Option<T>>`; malformed records must not be silently dropped.
- Strict mode fails on record parse errors.
- Tolerant mode skips malformed records and reports skipped counts.
- Worker, channel, ZIP-name, and output-write failures should propagate as typed `AppError` variants.
- Never write XML-derived group keys directly as ZIP entry names. Use `CsvZipSink::safe_entry_name` or an equivalent strict encoder.
- Do not commit private Apple Health exports or generated output ZIPs.

## Development Commands

Run the relevant narrow command while iterating, then run the full gate before handing work back:

```bash
cargo fmt --check
cargo check --locked --all-targets
cargo test --locked --all-targets
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo build --locked --benches
cargo tree --locked --duplicates
git diff --check
```

Use `cargo fmt` to format Rust source files before final verification.

## Benchmarking

- `cargo bench --bench flamegraph` benchmarks the CLI.
- By default the benchmark generates a synthetic Apple Health-style XML export so it does not depend on private data.
- Use `GPT_OS_BENCH_RECORDS=<count>` to scale the generated benchmark input.
- Use `GPT_OS_BENCH_INPUT=<path>` only for local, private real-export benchmarking.
- When a change touches `src/xml_utils.rs`, `src/core.rs`, `src/apple_health/types.rs`, or `src/sinks/csv_zip.rs`, compare performance against the relevant baseline before claiming success.

## Documentation

- When changing architecture, runtime behavior, command surfaces, or file structure, update `README.md` and/or `docs/PROJECT_STRUCTURE.md`.
- Keep docs factual and current. Do not leave guidance that points to removed files.
- Prefer consolidating guidance here over adding duplicate checklist files.

## Code Style

- Keep diffs focused and reviewable.
- Reuse existing patterns before introducing new abstractions.
- Preserve type safety and error visibility.
- Do not leave unused code.
- Avoid drive-by refactors outside the requested change.
