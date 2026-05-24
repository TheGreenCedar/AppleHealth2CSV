// This file contains integration tests for the application, verifying the functionality of the tool with sample inputs and expected outputs.

use std::collections::HashMap;
use std::fs;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use tempfile::{NamedTempFile, TempDir};
use zip::{ZipArchive, ZipWriter, write::FileOptions};

const SAMPLE_EXPORT: &str = "tests/fixtures/sample_export.xml";

#[test]
fn test_integration() {
    let (_dir, output_zip) = temp_output_path();

    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg(SAMPLE_EXPORT)
        .arg(&output_zip)
        .assert()
        .success();

    assert!(output_zip.exists());
    let output = read_zip(&output_zip);
    assert!(output.contains_key("HKQuantityTypeIdentifierBodyMass.csv"));
    assert!(output.contains_key("HKQuantityTypeIdentifierStepCount.csv"));
    assert!(output.contains_key("Workout.csv"));
    let steps = String::from_utf8(
        output
            .get("HKQuantityTypeIdentifierStepCount.csv")
            .expect("steps csv")
            .clone(),
    )
    .expect("utf8 csv");
    assert!(steps.contains("HKQuantityTypeIdentifierStepCount"));
    assert!(steps.contains("10000"));
}

#[test]
fn test_zipped_input_produces_same_output() {
    let (_xml_dir, xml_output) = temp_output_path();
    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg(SAMPLE_EXPORT)
        .arg(&xml_output)
        .assert()
        .success();

    let xml_data = fs::read(SAMPLE_EXPORT).expect("read xml");
    let mut zip_input = tempfile::Builder::new()
        .suffix(".zip")
        .tempfile()
        .expect("zip input");
    {
        let mut writer = ZipWriter::new(&mut zip_input);
        writer
            .start_file("export.xml", FileOptions::<()>::default())
            .expect("start file");
        writer.write_all(&xml_data).expect("write");
        writer.finish().expect("finish");
        zip_input
            .as_file_mut()
            .seek(std::io::SeekFrom::Start(0))
            .unwrap();
    }

    let (_zip_dir, zip_output) = temp_output_path();
    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg(zip_input.path())
        .arg(&zip_output)
        .assert()
        .success();

    let xml_map = read_zip(&xml_output);
    let zip_map = read_zip(&zip_output);
    assert_eq!(xml_map, zip_map);
}

#[test]
fn test_unknown_source_fails() {
    let (_dir, output_zip) = temp_output_path();

    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg("--source")
        .arg("unknown")
        .arg(SAMPLE_EXPORT)
        .arg(&output_zip)
        .assert()
        .failure();
}

#[test]
fn test_no_metrics_suppresses_stdout() {
    let (_dir, output_zip) = temp_output_path();

    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg("--no-metrics")
        .arg(SAMPLE_EXPORT)
        .arg(&output_zip)
        .assert()
        .success()
        .stdout("");
}

#[test]
fn test_missing_export_xml_in_zip_fails() {
    let mut zip_input = tempfile::Builder::new()
        .suffix(".zip")
        .tempfile()
        .expect("zip input");
    {
        let mut writer = ZipWriter::new(&mut zip_input);
        writer
            .start_file("not-export.xml", FileOptions::<()>::default())
            .expect("start file");
        writer
            .write_all(b"<HealthData></HealthData>")
            .expect("write");
        writer.finish().expect("finish");
        zip_input
            .as_file_mut()
            .seek(std::io::SeekFrom::Start(0))
            .unwrap();
    }

    let (_dir, output_zip) = temp_output_path();
    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg(zip_input.path())
        .arg(&output_zip)
        .assert()
        .failure();
}

#[test]
fn test_malformed_record_fails_in_strict_mode() {
    let xml = write_temp_xml(
        r#"<HealthData><Record type="Steps" type="Duplicate" value="1"/></HealthData>"#,
    );
    let (_dir, output_zip) = temp_output_path();

    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg(xml.path())
        .arg(&output_zip)
        .assert()
        .failure();
}

#[test]
fn test_malformed_record_can_be_skipped_in_tolerant_mode() {
    let xml = write_temp_xml(
        r#"<HealthData><Record type="Steps" type="Duplicate" value="1"/><Record type="Steps" value="2"/></HealthData>"#,
    );
    let (_dir, output_zip) = temp_output_path();

    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg("--tolerant")
        .arg(xml.path())
        .arg(&output_zip)
        .assert()
        .success();

    let output = read_zip(&output_zip);
    let steps =
        String::from_utf8(output.get("Steps.csv").expect("steps csv").clone()).expect("utf8 csv");
    assert!(steps.contains("2"));
}

#[test]
fn test_zip_entry_names_are_safe_for_untrusted_group_keys() {
    let xml = write_temp_xml(
        r#"<HealthData><Record type="../owned" value="1" startDate="2023-01-01T00:00:00Z"/></HealthData>"#,
    );
    let (_dir, output_zip) = temp_output_path();

    Command::cargo_bin("gpt-os")
        .expect("binary")
        .arg(xml.path())
        .arg(&output_zip)
        .assert()
        .success();

    let output = read_zip(&output_zip);
    assert!(!output.contains_key("../owned.csv"));
    assert!(output.contains_key("_2E_2E_2Fowned.csv"));
}

fn read_zip(path: &Path) -> HashMap<String, Vec<u8>> {
    let file = fs::File::open(path).expect("open zip");
    let mut archive = ZipArchive::new(file).expect("open archive");
    let mut map = HashMap::new();
    for i in 0..archive.len() {
        let mut f = archive.by_index(i).expect("entry");
        let mut data = Vec::new();
        f.read_to_end(&mut data).expect("read");
        map.insert(f.name().to_string(), data);
    }
    map
}

fn write_temp_xml(contents: &str) -> NamedTempFile {
    let mut file = tempfile::Builder::new()
        .suffix(".xml")
        .tempfile()
        .expect("temp xml");
    file.write_all(contents.as_bytes()).expect("write xml");
    file
}

fn temp_output_path() -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("output.zip");
    (dir, path)
}
