use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

const DEFAULT_SYNTHETIC_RECORDS: usize = 100_000;
const RECORD_TYPES: [&str; 8] = [
    "HKQuantityTypeIdentifierStepCount",
    "HKQuantityTypeIdentifierHeartRate",
    "HKQuantityTypeIdentifierActiveEnergyBurned",
    "HKQuantityTypeIdentifierDistanceWalkingRunning",
    "HKCategoryTypeIdentifierSleepAnalysis",
    "HKQuantityTypeIdentifierBodyMass",
    "HKQuantityTypeIdentifierFlightsClimbed",
    "HKQuantityTypeIdentifierWalkingSpeed",
];

struct BenchInput {
    _dir: Option<tempfile::TempDir>,
    path: PathBuf,
    records: Option<usize>,
    label: String,
}

fn bench_sample(c: &mut Criterion) {
    let input = bench_input();
    let bench_name = format!("process_{}", input.label);

    let mut group = c.benchmark_group("cli");
    if let Some(records) = input.records {
        group.throughput(Throughput::Elements(records as u64));
    }

    group.bench_function(&bench_name, |b| {
        b.iter(|| {
            let output_dir = tempfile::tempdir().expect("temp dir");
            let output = output_dir.path().join("bench-output.zip");
            let status = Command::new(env!("CARGO_BIN_EXE_gpt-os"))
                .arg("--no-metrics")
                .arg(&input.path)
                .arg(&output)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .expect("failed to execute process");
            assert!(status.success());
        });
    });
    group.finish();
}

fn bench_input() -> BenchInput {
    if let Ok(input) = env::var("GPT_OS_BENCH_INPUT") {
        return BenchInput {
            _dir: None,
            path: PathBuf::from(input),
            records: None,
            label: "provided_export".to_string(),
        };
    }

    let records = env::var("GPT_OS_BENCH_RECORDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_SYNTHETIC_RECORDS);
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("synthetic_export.xml");
    write_synthetic_export(&path, records).expect("write synthetic export");

    BenchInput {
        _dir: Some(dir),
        path,
        records: Some(records),
        label: format!("synthetic_{records}_records"),
    }
}

fn write_synthetic_export(path: &Path, records: usize) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writeln!(writer, r#"<HealthData locale="en_US">"#)?;

    for i in 0..records {
        let record_type = RECORD_TYPES[i % RECORD_TYPES.len()];
        let day = (i % 28) + 1;
        let minute = i % 60;
        let value = (i % 10_000) + 1;

        writeln!(
            writer,
            r#"<Record type="{record_type}" sourceName="Synthetic" sourceVersion="1" unit="count" creationDate="2024-01-{day:02} 00:{minute:02}:00 -0500" startDate="2024-01-{day:02} 00:{minute:02}:00 -0500" endDate="2024-01-{day:02} 00:{minute:02}:30 -0500" value="{value}"/>"#
        )?;
    }

    writeln!(writer, "</HealthData>")?;
    writer.flush()
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(15))
        .sample_size(10);
    targets = bench_sample
}
criterion_main!(benches);
