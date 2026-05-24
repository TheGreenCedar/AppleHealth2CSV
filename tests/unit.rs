use ahash::AHashMap;
use async_trait::async_trait;
use gpt_os::apple_health::policy::AppleHealthTransformPolicy;
use gpt_os::apple_health::types::GenericRecord;
use gpt_os::core::{Engine, ExtractEvent, Extractor, Sink};
use gpt_os::error::Result;
use gpt_os::record::RecordProjection;
use gpt_os::sinks::csv_zip::CsvZipSink;
use gpt_os::transform::TransformPolicy;
use quick_xml::Reader;
use quick_xml::events::Event;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tempfile::NamedTempFile;
use tokio::sync::mpsc;
use tokio_test::block_on;
use zip::ZipArchive;

type CapturedGenericGroups = Arc<Mutex<Vec<(String, Vec<GenericRecord>)>>>;

#[test]
fn materialized_engine_runs_synthetic_pipeline() {
    #[derive(Debug, Clone)]
    struct TestRecord {
        group: &'static str,
        sort: &'static str,
        value: &'static str,
    }

    impl RecordProjection for TestRecord {
        fn field_names(&self) -> impl Iterator<Item = &str> {
            ["group", "sort", "value"].into_iter()
        }

        fn field_value(&self, name: &str) -> Option<&str> {
            match name {
                "group" => Some(self.group),
                "sort" => Some(self.sort),
                "value" => Some(self.value),
                _ => None,
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    struct TestPolicy;

    impl TransformPolicy<TestRecord> for TestPolicy {
        fn group_key(&self, record: &TestRecord) -> String {
            record.group.to_string()
        }

        fn sort_key<'a>(&self, record: &'a TestRecord) -> Option<&'a str> {
            Some(record.sort)
        }
    }

    #[derive(Debug, Clone)]
    struct TestExtractor {
        records: Vec<TestRecord>,
    }

    #[async_trait]
    impl Extractor<TestRecord> for TestExtractor {
        async fn extract(
            &self,
            _input_path: &Path,
        ) -> Result<mpsc::Receiver<Result<ExtractEvent<TestRecord>>>> {
            let (tx, rx) = mpsc::channel(8);
            for record in self.records.clone() {
                tx.send(Ok(ExtractEvent::Record(record))).await.unwrap();
            }
            drop(tx);
            Ok(rx)
        }
    }

    type CapturedTestGroups = Arc<Mutex<Vec<(String, Vec<TestRecord>)>>>;

    #[derive(Debug, Clone)]
    struct TestSink {
        captured: CapturedTestGroups,
    }

    #[async_trait]
    impl Sink<TestRecord> for TestSink {
        async fn load(
            &self,
            grouped_records: AHashMap<String, Vec<TestRecord>>,
            _output_path: &Path,
        ) -> Result<()> {
            let mut entries: Vec<_> = grouped_records.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            *self.captured.lock().unwrap() = entries;
            Ok(())
        }
    }

    let captured = Arc::new(Mutex::new(Vec::new()));
    let engine = Engine::new(
        TestExtractor {
            records: vec![
                TestRecord {
                    group: "b",
                    sort: "2",
                    value: "late",
                },
                TestRecord {
                    group: "b",
                    sort: "1",
                    value: "early",
                },
            ],
        },
        TestPolicy,
        TestSink {
            captured: captured.clone(),
        },
    );

    let tmp = NamedTempFile::new().unwrap();
    let report = block_on(engine.run(Path::new("ignored"), tmp.path())).unwrap();
    assert_eq!(report.total_records, 2);
    let captured = captured.lock().unwrap();
    assert_eq!(captured[0].0, "b");
    assert_eq!(captured[0].1[0].value, "early");
    assert_eq!(captured[0].1[1].value, "late");
}

#[test]
fn record_from_xml_optional_fields() {
    let xml = r#"<Record type="Heart" value="60" creationDate="2020" startDate="2020" endDate="2020" sourceName="watch"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let rec = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(rec.element_name, "Record");
            assert_eq!(rec.attributes.get("type").unwrap(), "Heart");
            assert_eq!(rec.attributes.get("value").unwrap(), "60");
            assert_eq!(rec.attributes.get("unit"), None);
            assert_eq!(rec.attributes.get("sourceVersion"), None);
            assert_eq!(rec.attributes.get("device"), None);
        }
        _ => panic!("Expected empty Record event"),
    }
}

#[test]
fn workout_from_xml_numeric_fields() {
    let xml = r#"<Workout workoutActivityType="Run" duration="42.5" totalDistance="5.2" totalEnergyBurned="300" sourceName="watch" startDate="2020" endDate="2020"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let workout = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(workout.element_name, "Workout");
            assert_eq!(
                workout.attributes.get("workoutActivityType").unwrap(),
                "Run"
            );
            assert_eq!(workout.attributes.get("duration").unwrap(), "42.5");
            assert_eq!(workout.attributes.get("totalDistance").unwrap(), "5.2");
            assert_eq!(workout.attributes.get("totalEnergyBurned").unwrap(), "300");
            assert_eq!(workout.attributes.get("device"), None);
        }
        _ => panic!("Expected empty Workout event"),
    }
}

#[test]
fn activity_summary_from_xml_numeric_fields() {
    let xml = r#"<ActivitySummary dateComponents="2023-01-01" activeEnergyBurned="300" activeEnergyBurnedGoal="500" appleExerciseTime="30" appleStandHours="12"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let summary = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(summary.element_name, "ActivitySummary");
            assert_eq!(
                summary.attributes.get("dateComponents").unwrap(),
                "2023-01-01"
            );
            assert_eq!(summary.attributes.get("activeEnergyBurned").unwrap(), "300");
            assert_eq!(
                summary.attributes.get("activeEnergyBurnedGoal").unwrap(),
                "500"
            );
            assert_eq!(summary.attributes.get("appleExerciseTime").unwrap(), "30");
            assert_eq!(summary.attributes.get("appleStandHours").unwrap(), "12");
        }
        _ => panic!("Expected empty ActivitySummary event"),
    }
}

#[test]
fn generic_record_from_xml() {
    let xml = r#"<Correlation type="Blood" startDate="2020" endDate="2020"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let g = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(g.element_name, "Correlation");
            assert_eq!(g.attributes.get("type").unwrap(), "Blood");
            assert_eq!(g.attributes.get("startDate").unwrap(), "2020");
        }
        _ => panic!("Expected empty event"),
    }
}

#[test]
fn generic_record_grouping_key_for_record() {
    let xml = r#"<Record type="HKQuantityTypeIdentifierBodyMass" value="70" startDate="2020" endDate="2020" creationDate="2020" sourceName="watch"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let g = GenericRecord::from_xml(&e).unwrap();
            let policy = AppleHealthTransformPolicy;
            assert_eq!(policy.group_key(&g), "HKQuantityTypeIdentifierBodyMass");
        }
        _ => panic!("Expected empty Record event"),
    }
}

#[test]
fn xml_attribute_values_are_unescaped() {
    let xml = r#"<Record type="Steps" sourceName="A &amp; B" startDate="2023-01-01T00:00:00Z"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut buf = Vec::new();
    match reader.read_event_into(&mut buf).unwrap() {
        Event::Empty(e) => {
            let rec = GenericRecord::from_xml(&e).unwrap();
            assert_eq!(rec.attributes.get("sourceName").unwrap(), "A & B");
        }
        _ => panic!("expected empty"),
    }
}

#[test]
fn materialized_engine_sorts_records_by_policy() {
    let xml1 =
        r#"<Record type="Steps" startDate="2023-01-02T00:00:00Z" endDate="2023-01-02T00:00:00Z"/>"#;
    let xml2 =
        r#"<Record type="Steps" startDate="2023-01-01T00:00:00Z" endDate="2023-01-01T00:00:00Z"/>"#;

    let parse = |xml: &str| {
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);
        let mut buf = Vec::new();
        match reader.read_event_into(&mut buf).unwrap() {
            Event::Empty(e) => GenericRecord::from_xml(&e).unwrap(),
            _ => panic!("expected empty"),
        }
    };

    let r1 = parse(xml1);
    let r2 = parse(xml2);

    let policy = AppleHealthTransformPolicy;
    assert_eq!(policy.sort_key(&r1), Some("2023-01-02T00:00:00Z"));
    assert_eq!(policy.sort_key(&r2), Some("2023-01-01T00:00:00Z"));

    let captured = Arc::new(Mutex::new(Vec::new()));
    let extractor = StaticExtractor {
        records: vec![r1, r2],
    };
    let sink = CaptureSink {
        captured: captured.clone(),
    };
    let engine = Engine::new(extractor, policy, sink);

    let tmp = NamedTempFile::new().unwrap();
    block_on(engine.run(Path::new("ignored.xml"), tmp.path())).unwrap();

    let captured = captured.lock().unwrap();
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].0, "Steps");
    let rows = &captured[0].1;
    assert_eq!(
        rows[0].attributes.get("startDate").unwrap(),
        "2023-01-01T00:00:00Z"
    );
    assert_eq!(
        rows[1].attributes.get("startDate").unwrap(),
        "2023-01-02T00:00:00Z"
    );
}

#[test]
fn csv_sink_writes_projected_records() {
    let xml1 =
        r#"<Record type="Steps" startDate="2023-01-01T00:00:00Z" endDate="2023-01-01T00:00:00Z"/>"#;
    let parse = |xml: &str| {
        let mut reader = Reader::from_str(xml);
        reader.config_mut().trim_text(true);
        let mut buf = Vec::new();
        match reader.read_event_into(&mut buf).unwrap() {
            Event::Empty(e) => GenericRecord::from_xml(&e).unwrap(),
            _ => panic!("expected empty"),
        }
    };

    let mut map: AHashMap<String, Vec<GenericRecord>> = AHashMap::new();
    map.entry("Steps".to_string())
        .or_default()
        .push(parse(xml1));

    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("output.zip");
    block_on(CsvZipSink::default().load(map, &output)).unwrap();

    let file = File::open(output).unwrap();
    let mut archive = ZipArchive::new(file).unwrap();
    let mut f = archive.by_index(0).unwrap();
    let mut csv_data = String::new();
    f.read_to_string(&mut csv_data).unwrap();
    let lines: Vec<&str> = csv_data.lines().collect();
    assert!(lines[1].contains("2023-01-01T00:00:00Z"));
}

#[test]
fn csv_zip_entry_names_are_encoded() {
    assert_eq!(
        CsvZipSink::safe_entry_name("../owned").unwrap(),
        "_2E_2E_2Fowned.csv"
    );
    assert_eq!(
        CsvZipSink::safe_entry_name(r"C:\owned").unwrap(),
        "C_3A_5Cowned.csv"
    );
}

#[derive(Debug, Clone)]
struct StaticExtractor {
    records: Vec<GenericRecord>,
}

#[async_trait]
impl Extractor<GenericRecord> for StaticExtractor {
    async fn extract(
        &self,
        _input_path: &Path,
    ) -> Result<mpsc::Receiver<Result<ExtractEvent<GenericRecord>>>> {
        let (tx, rx) = mpsc::channel(8);
        for record in self.records.clone() {
            tx.send(Ok(ExtractEvent::Record(record))).await.unwrap();
        }
        drop(tx);
        Ok(rx)
    }
}

#[derive(Debug, Clone)]
struct CaptureSink {
    captured: CapturedGenericGroups,
}

#[async_trait]
impl Sink<GenericRecord> for CaptureSink {
    async fn load(
        &self,
        grouped_records: AHashMap<String, Vec<GenericRecord>>,
        _output_path: &Path,
    ) -> Result<()> {
        let mut entries: Vec<(String, Vec<GenericRecord>)> = grouped_records.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        *self.captured.lock().unwrap() = entries;
        Ok(())
    }
}
