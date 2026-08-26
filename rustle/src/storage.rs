use anyhow::{Context, Result};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, NaiveDate, Utc};
use parquet::{
    arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter},
    basic::{Compression, ZstdLevel},
    file::properties::WriterProperties,
};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
};
use uuid::Uuid;

/// Each Parquet row is a versioned normalized JSON record. This avoids lossy schema changes;
/// partition location carries dataset/date/market and every payload carries Meta.
pub fn write<T: Serialize>(
    root: &Path,
    dataset: &str,
    market: &str,
    ts: DateTime<Utc>,
    records: &[T],
) -> Result<Option<PathBuf>> {
    if records.is_empty() {
        return Ok(None);
    }
    let dir = root
        .join(dataset)
        .join(format!("date={}", ts.format("%F")))
        .join(format!("market={market}"));
    fs::create_dir_all(&dir)?;
    let path = dir.join(format!("part-{}.parquet", Uuid::new_v4()));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        DataType::Utf8,
        false,
    )]));
    let values: Vec<String> = records
        .iter()
        .map(serde_json::to_string)
        .collect::<std::result::Result<_, _>>()?;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(values)) as ArrayRef],
    )?;
    let file = File::create(&path)?;
    // The payload column is JSON text with a repeated field-name skeleton, so it compresses
    // ~34x. Left uncompressed a 28-day collection is ~542 GB; it is ~16 GB compressed.
    // Level 3 is zstd's default and runs orders of magnitude faster than the stream it records.
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(Some(path))
}
pub fn read_all<T: DeserializeOwned>(root: &Path, dataset: &str) -> Result<Vec<T>> {
    let base = root.join(dataset);
    if !base.exists() {
        return Ok(vec![]);
    }
    let mut files = vec![];
    walk(&base, &mut files)?;
    read_parquet_files(files)
}

fn read_parquet_files<T: DeserializeOwned>(files: Vec<PathBuf>) -> Result<Vec<T>> {
    let mut out = vec![];
    for p in files
        .into_iter()
        .filter(|p| p.extension().is_some_and(|x| x == "parquet"))
    {
        let f = File::open(p)?;
        let b = ParquetRecordBatchReaderBuilder::try_new(f)?
            .with_batch_size(4096)
            .build()?;
        for batch in b {
            let batch = batch?;
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("payload column")?;
            for i in 0..col.len() {
                out.push(serde_json::from_str(col.value(i))?);
            }
        }
    }
    Ok(out)
}
/// The UTC dates a dataset has partitions for, ascending. Reads directory names only:
/// the partition layout already encodes the date, so selecting one costs a `read_dir`
/// rather than a parse of every record.
pub fn dataset_dates(root: &Path, dataset: &str) -> Result<Vec<NaiveDate>> {
    let base = root.join(dataset);
    if !base.exists() {
        return Ok(vec![]);
    }
    let mut dates = vec![];
    for entry in fs::read_dir(&base)? {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .context("partition name is not valid UTF-8")?;
        // A partition we cannot read is a missing collection day. Skipping it silently
        // would shrink the gate window without saying so.
        let raw = name
            .strip_prefix("date=")
            .ok_or_else(|| anyhow::anyhow!("unexpected partition {name} in dataset {dataset}"))?;
        dates.push(
            NaiveDate::parse_from_str(raw, "%F").with_context(|| {
                format!("unparseable partition date {name} in dataset {dataset}")
            })?,
        );
    }
    dates.sort();
    Ok(dates)
}

/// One UTC partition of a dataset. The chunk unit for `analyze`.
pub fn read_date<T: DeserializeOwned>(
    root: &Path,
    dataset: &str,
    date: NaiveDate,
) -> Result<Vec<T>> {
    let base = root
        .join(dataset)
        .join(format!("date={}", date.format("%F")));
    if !base.exists() {
        return Ok(vec![]);
    }
    let mut files = vec![];
    walk(&base, &mut files)?;
    read_parquet_files(files)
}

/// Replace a derived dataset. Raw collection datasets are never replaced by this helper.
pub fn clear_dataset(root: &Path, dataset: &str) -> Result<()> {
    let path = root.join(dataset);
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

/// Alert delivery is intentionally dependency-free and append-only.  Unlike
/// Parquet batches this makes each emitted alert immediately tail-able.
pub fn append_jsonl<T: Serialize>(
    root: &Path,
    dataset: &str,
    ts: DateTime<Utc>,
    record: &T,
) -> Result<PathBuf> {
    let dir = root.join(dataset).join(format!("date={}", ts.format("%F")));
    fs::create_dir_all(&dir)?;
    let path = dir.join("events.jsonl");
    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)?;
    writeln!(file, "{}", serde_json::to_string(record)?)?;
    Ok(path)
}
fn walk(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for e in fs::read_dir(dir)? {
        let p = e?.path();
        if p.is_dir() {
            walk(&p, out)?
        } else {
            out.push(p)
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use parquet::basic::Compression;

    /// One market-day of similar records, which is what the collector actually writes.
    fn payloads(count: usize) -> Vec<serde_json::Value> {
        (0..count)
            .map(|i| {
                serde_json::json!({
                    "meta": {
                        "schema_version": 1,
                        "market": "KRW-TEST",
                        "exchange_ts": "2025-01-01T00:00:00Z",
                        "receive_ts": "2025-01-01T00:00:00Z",
                    },
                    "total_ask_size": 1000.0 + i as f64,
                    "total_bid_size": 900.0 + i as f64,
                })
            })
            .collect()
    }

    fn at(y: i32, m: u32, d: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, 12, 0, 0).unwrap()
    }

    #[test]
    fn written_parquet_is_compressed_and_still_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let records = payloads(500);
        let raw_json_bytes: usize = records
            .iter()
            .map(|r| serde_json::to_string(r).unwrap().len())
            .sum();

        let path = write(
            dir.path(),
            "orderbooks",
            "KRW-TEST",
            at(2025, 1, 1),
            &records,
        )
        .unwrap()
        .unwrap();

        let read_back: Vec<serde_json::Value> = read_all(dir.path(), "orderbooks").unwrap();
        assert_eq!(read_back, records, "compression must not alter payloads");

        let on_disk = std::fs::metadata(&path).unwrap().len() as usize;
        assert!(
            on_disk * 4 < raw_json_bytes,
            "expected compression well past 4x, got {on_disk} bytes from {raw_json_bytes} of JSON"
        );

        let meta = ParquetRecordBatchReaderBuilder::try_new(File::open(&path).unwrap())
            .unwrap()
            .metadata()
            .clone();
        assert_ne!(
            meta.row_group(0).column(0).compression(),
            Compression::UNCOMPRESSED,
            "payload column must declare a compression codec"
        );
    }

    #[test]
    fn partitions_written_before_compression_still_read() {
        // Parquet records its codec per column chunk, so the 2026-08-26 partition collected
        // before this change must keep loading. There is no migration and none is needed.
        let dir = tempfile::tempdir().unwrap();
        let records = payloads(10);
        let target = dir
            .path()
            .join("orderbooks/date=2025-01-01/market=KRW-TEST");
        fs::create_dir_all(&target).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "payload",
            DataType::Utf8,
            false,
        )]));
        let values: Vec<String> = records
            .iter()
            .map(|r| serde_json::to_string(r).unwrap())
            .collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(values)) as ArrayRef],
        )
        .unwrap();
        let mut writer = ArrowWriter::try_new(
            File::create(target.join("part-legacy.parquet")).unwrap(),
            schema,
            None,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let read_back: Vec<serde_json::Value> = read_all(dir.path(), "orderbooks").unwrap();
        assert_eq!(read_back, records);
    }
}
