use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{Timelike, Utc};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use crate::config::ParquetSinkConfig;
use crate::events::Event;
use crate::ops::health::HealthRegistry;
use crate::ops::metrics::ParquetMetrics;


pub async fn run(
    config: ParquetSinkConfig,
    data_dir: PathBuf,
    mut rx: mpsc::Receiver<Event>,
    health: HealthRegistry,
    metrics: ParquetMetrics,
    cancel: CancellationToken,
) {
    let mut buffers: HashMap<String, Vec<Event>> = HashMap::new();
    let mut last_flush: HashMap<String, Instant> = HashMap::new();
    let flush_interval = Duration::from_millis(config.flush_interval_ms);
    let mut check_interval = tokio::time::interval(Duration::from_secs(10));
    let version = &config.schema_version;

    info!("parquet sink started");

    loop {
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(event) => {
                        let key = buffer_key(&event);
                        let buf = buffers.entry(key.clone()).or_default();
                        buf.push(event);
                        last_flush.entry(key.clone()).or_insert_with(Instant::now);

                        if buf.len() >= config.buffer_rows {
                            flush_buffer(
                                &key, buf, &data_dir, version,
                                &config, &health, &metrics,
                            );
                            buf.clear();
                            last_flush.insert(key, Instant::now());
                        }
                    }
                    None => break,
                }
            }
            _ = check_interval.tick() => {
                let now = Instant::now();
                let keys_to_flush: Vec<String> = last_flush
                    .iter()
                    .filter(|(_, last)| now.duration_since(**last) >= flush_interval)
                    .map(|(k, _)| k.clone())
                    .collect();
                for key in keys_to_flush {
                    if let Some(buf) = buffers.get_mut(&key) {
                        if !buf.is_empty() {
                            flush_buffer(
                                &key, buf, &data_dir, version,
                                &config, &health, &metrics,
                            );
                            buf.clear();
                            last_flush.insert(key, Instant::now());
                        }
                    }
                }
            }
            _ = cancel.cancelled() => break,
        }
    }

    // Drain remaining
    while let Ok(event) = rx.try_recv() {
        let key = buffer_key(&event);
        buffers.entry(key).or_default().push(event);
    }

    // Final flush
    info!("parquet sink flushing on shutdown ({} buffers)", buffers.len());
    for (key, buf) in &mut buffers {
        if !buf.is_empty() {
            flush_buffer(key, buf, &data_dir, version, &config, &health, &metrics);
        }
    }

    info!("parquet sink shutdown complete");
}

fn buffer_key(event: &Event) -> String {
    let now = Utc::now();
    let date = now.format("%Y-%m-%d").to_string();
    let hour = now.hour();
    let event_type = event.event_type();
    let venue = event
        .venue()
        .map(|v| v.to_string())
        .unwrap_or_else(|| "multi".into());
    format!("{event_type}/{venue}/{date}/{hour}")
}

fn flush_buffer(
    key: &str,
    events: &[Event],
    data_dir: &Path,
    version: &str,
    config: &ParquetSinkConfig,
    health: &HealthRegistry,
    metrics: &ParquetMetrics,
) {
    if events.is_empty() {
        return;
    }

    let start = Instant::now();
    let seq = chrono::Utc::now().timestamp_millis();
    let dir = data_dir.join(version).join(key);
    if let Err(e) = fs::create_dir_all(&dir) {
        error!(error = %e, path = %dir.display(), "failed to create parquet directory");
        return;
    }

    let tmp_path = dir.join(format!("{seq}.parquet.tmp"));
    let final_path = dir.join(format!("{seq}.parquet"));

    match write_parquet_file(&tmp_path, events, config) {
        Ok(row_count) => {
            if let Err(e) = fs::rename(&tmp_path, &final_path) {
                error!(error = %e, "failed to rename parquet file");
                return;
            }
            metrics.files_written_total.inc();
            let event_type = events[0].event_type();
            metrics
                .rows_written_total
                .with_label_values(&[event_type])
                .inc_by(row_count as u64);
            let elapsed = start.elapsed().as_millis() as f64;
            metrics
                .flush_duration_ms
                .with_label_values(&[event_type])
                .observe(elapsed);
            health.set_last_parquet_flush(Utc::now());
            debug!(
                path = %final_path.display(),
                rows = row_count,
                elapsed_ms = elapsed,
                "parquet file written"
            );
        }
        Err(e) => {
            error!(error = %e, "failed to write parquet file");
            let _ = fs::remove_file(&tmp_path);
        }
    }
}

fn write_parquet_file(
    path: &Path,
    events: &[Event],
    config: &ParquetSinkConfig,
) -> anyhow::Result<usize> {
    // Generic schema: store as JSON lines with metadata columns
    let schema = Arc::new(Schema::new(vec![
        Field::new("seq", DataType::Int64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new(
            "recv_ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("venue", DataType::Utf8, true),
        Field::new("payload", DataType::Utf8, false),
    ]));

    let mut seq_builder = Int64Builder::with_capacity(events.len());
    let mut type_builder = StringBuilder::with_capacity(events.len(), events.len() * 16);
    let mut ts_builder = TimestampMicrosecondBuilder::with_capacity(events.len());
    let mut venue_builder = StringBuilder::with_capacity(events.len(), events.len() * 10);
    let mut payload_builder = StringBuilder::with_capacity(events.len(), events.len() * 256);

    for event in events {
        let meta = event.meta();
        seq_builder.append_value(meta.seq as i64);
        type_builder.append_value(event.event_type());
        ts_builder.append_value(meta.recv_ts.timestamp_micros());
        match event.venue() {
            Some(v) => venue_builder.append_value(v.to_string()),
            None => venue_builder.append_null(),
        }
        let json = serde_json::to_string(event).unwrap_or_default();
        payload_builder.append_value(&json);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(seq_builder.finish()),
            Arc::new(type_builder.finish()),
            Arc::new(ts_builder.finish().with_timezone("UTC")),
            Arc::new(venue_builder.finish()),
            Arc::new(payload_builder.finish()),
        ],
    )?;

    let file = fs::File::create(path)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_key_value_metadata(Some(vec![
            parquet::format::KeyValue {
                key: "schema_version".into(),
                value: Some(config.schema_version.clone()),
            },
            parquet::format::KeyValue {
                key: "row_count".into(),
                value: Some(events.len().to_string()),
            },
            parquet::format::KeyValue {
                key: "first_recv_ts".into(),
                value: events.first().map(|e| e.meta().recv_ts.to_rfc3339()),
            },
            parquet::format::KeyValue {
                key: "last_recv_ts".into(),
                value: events.last().map(|e| e.meta().recv_ts.to_rfc3339()),
            },
        ]))
        .build();

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(events.len())
}
