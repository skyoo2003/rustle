use anyhow::{Context, Result};
use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::{DateTime, Utc};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
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
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
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
