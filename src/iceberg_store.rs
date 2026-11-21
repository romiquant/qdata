//! Iceberg/Parquet 读后端：从 warehouse 数据目录直读 kline，产出与
//! `TDEngine::query_kline` 完全一致的 `KlineSeries` 流（同分组、同排序、
//! 同 `source != 1` 过滤、同 [start, end] 闭区间语义），下游 qfq/指标/
//! Flight 序列化逻辑不变。由 `KLINE_BACKEND=iceberg` + `ICEBERG_KLINE_DIR`
//! 启用，用于存储层选型基准。
//!
//! 目录布局（icebergarchive 的 identity 分区 + 月分区）：
//!   `{data_root}/exchange=<ex>/freq=<freq>/ts_month=<YYYY-MM>/*.parquet`

use crate::flight_service::DatasetTicket;
use crate::kline_processor::KlineSeries;
use anyhow::{anyhow, Result};
use arrow::array::{
    Array, BooleanArray, Float64Array, Int32Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use arrow::datatypes::DataType;
use arrow::compute::cast;
use chrono::Datelike;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};

/// 与 SQL 行一一对应的扁平行。
#[derive(Clone)]
struct Row {
    ts: i64,    // ms（与 TDengine raw ts 一致）
    open: f64,
    close: f64,
    high: f64,
    low: f64,
    vol: f64,
    qv: f64,
    end_ts: i64, // ms
}

pub struct IcebergStore {
    data_root: String,
    read_concurrency: usize,
}

async fn send_or_warn(tx: &mpsc::Sender<Result<KlineSeries>>, result: Result<KlineSeries>) -> bool {
    if tx.send(result).await.is_err() {
        tracing::warn!("Receiver dropped, stopping query");
        return false;
    }
    true
}

/// 一个月目录名 `ts_month=YYYY-MM` → [月首, 次月首) 的 ms 区间。
fn parse_month_dir(name: &str) -> Option<(i64, i64)> {
    let rest = name.strip_prefix("ts_month=")?;
    let (y, m) = rest.split_once('-')?;
    let y: i32 = y.parse().ok()?;
    let m: u32 = m.parse().ok()?;
    let start = chrono::NaiveDate::from_ymd_opt(y, m, 1)?
        .and_hms_opt(0, 0, 0)?
        .and_utc()
        .timestamp_millis();
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    let end = chrono::NaiveDate::from_ymd_opt(ny, nm, 1)?
        .and_hms_opt(0, 0, 0)?
        .and_utc()
        .timestamp_millis();
    Some((start, end))
}

fn month_dir_for_ms(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let msec = ms.rem_euclid(1000);
    let dt = chrono::DateTime::from_timestamp(secs, msec as u32 * 1_000_000)
        .expect("ts out of range")
        .naive_utc();
    format!("ts_month={:04}-{:02}", dt.year(), dt.month())
}

impl IcebergStore {
    pub fn new(data_root: String) -> Self {
        Self {
            data_root,
            read_concurrency: 8,
        }
    }

    /// 与 TDEngine::query_kline 同契约：查询 [start, end] 闭区间、
    /// source != 1，按 (exchange, symbol, freq) 分组、组内 ts 升序，
    /// 通过 tx 逐组发送 KlineSeries。
    pub async fn query_kline(
        &self,
        ticket: &DatasetTicket,
        tx: mpsc::Sender<Result<KlineSeries>>,
    ) -> Result<()> {
        let start = ticket.start_time;
        let end = ticket.end_time;

        let mut exchanges = HashSet::new();
        let mut symbols = HashSet::new();
        let mut freqs = HashSet::new();
        for tag in &ticket.tags {
            let (exchange, symbol, freq) = DatasetTicket::parse_tag(tag)?;
            exchanges.insert(exchange.to_string());
            symbols.insert(symbol.to_string());
            freqs.insert(freq.to_string());
        }

        // 枚举命中的分区目录下的 parquet 文件
        let mut files: Vec<PathBuf> = Vec::new();
        for exchange in &exchanges {
            for freq in &freqs {
                let pdir = Path::new(&self.data_root)
                    .join(format!("exchange={exchange}"))
                    .join(format!("freq={freq}"));
                let Ok(rd) = std::fs::read_dir(&pdir) else {
                    continue;
                };
                for entry in rd.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if let Some((mstart, mend)) = parse_month_dir(&name) {
                        // 目录内 ts ∈ [mstart, mend)，与 [start, end] 求交
                        if mstart <= end && mend - 1 >= start {
                            if let Ok(frd) = std::fs::read_dir(entry.path()) {
                                for f in frd.flatten() {
                                    if f.path().extension().is_some_and(|e| e == "parquet") {
                                        files.push(f.path());
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let query_start = std::time::Instant::now();
        let symbols_arc = Arc::new(symbols);
        let sem = Arc::new(Semaphore::new(self.read_concurrency));

        // 先并行读 footer 拿每个文件的 row group 数，再按 row group 粒度并行解码
        let mut meta_handles = Vec::with_capacity(files.len());
        for file in files {
            let permit = sem.clone().acquire_owned().await?;
            meta_handles.push(tokio::task::spawn_blocking(move || {
                let _permit = permit;
                row_group_count(&file)
            }));
        }
        let mut rg_tasks = Vec::new();
        for handle in meta_handles {
            let (path, num_rgs) = handle.await.map_err(|e| anyhow!("join error: {e}"))??;
            for rg in 0..num_rgs {
                let permit = sem.clone().acquire_owned().await?;
                let symbols = symbols_arc.clone();
                let path = path.clone();
                rg_tasks.push(tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    read_row_group(&path, rg, start, end, &symbols)
                }));
            }
        }

        // (exchange, symbol, freq) -> rows；BTreeMap 保证字典序输出，
        // 与 SQL 的 ORDER BY exchange, symbol, freq 一致
        let mut grouped: BTreeMap<(String, String, String), Vec<Row>> = BTreeMap::new();
        let mut first_data_logged = false;
        for handle in rg_tasks {
            let rows = handle.await.map_err(|e| anyhow!("join error: {e}"))??;
            if !first_data_logged {
                first_data_logged = true;
                tracing::info!(
                    "Time from query start to first file data: {} ms",
                    query_start.elapsed().as_millis()
                );
            }
            for (key, mut rs) in rows {
                rs.sort_by_key(|r| r.ts);
                grouped.entry(key).or_default().extend(rs);
            }
        }

        for ((exchange, symbol, freq), mut rows) in grouped {
            // 行来自多个月份文件的并行任务，完成顺序不定，必须全局排序
            rows.sort_by_key(|r| r.ts);
            let mut series = KlineSeries::new(exchange, symbol, freq);
            for r in rows {
                series.push(r.ts, r.open, r.close, r.high, r.low, r.vol, r.qv, r.end_ts);
            }
            if !send_or_warn(&tx, Ok(series)).await {
                return Ok(());
            }
        }

        if !first_data_logged {
            tracing::info!("No data found for query, returning empty series");
            // 与 TDengine 路径一致：发送空 series 保证 Flight 流含 schema 消息
            if let Some((exchange, symbol, freq)) = ticket.tags.first().and_then(|t| {
                DatasetTicket::parse_tag(t)
                    .ok()
                    .map(|(e, s, f)| (e.to_string(), s.to_string(), f.to_string()))
            }) {
                send_or_warn(&tx, Ok(KlineSeries::new(exchange, symbol, freq))).await;
            }
        }
        tracing::info!("last_data_time: {} ms", query_start.elapsed().as_millis());
        Ok(())
    }
}

/// 读 footer 返回文件内的 row group 数。
fn row_group_count(path: &Path) -> Result<(PathBuf, usize)> {
    let file = std::fs::File::open(path).map_err(|e| anyhow!("open {}: {e}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok((path.to_path_buf(), builder.metadata().num_row_groups()))
}

/// 读单个 row group，应用 [start,end] / source!=1 / symbol IN 过滤，
/// 按 (exchange, symbol, freq) 分组返回。
fn read_row_group(
    path: &Path,
    rg: usize,
    start: i64,
    end: i64,
    symbols: &HashSet<String>,
) -> Result<BTreeMap<(String, String, String), Vec<Row>>> {
    let file = std::fs::File::open(path).map_err(|e| anyhow!("open {}: {e}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder
        .with_batch_size(16384)
        .with_row_groups(vec![rg])
        .build()?;

    let mut out: BTreeMap<(String, String, String), Vec<Row>> = BTreeMap::new();
    for batch in reader {
        let batch = batch.map_err(|e| anyhow!("read {}: {e}", path.display()))?;
        filter_batch_into(&batch, start, end, symbols, &mut out)?;
    }
    Ok(out)
}

fn filter_batch_into(
    batch: &RecordBatch,
    start: i64,
    end: i64,
    symbols: &HashSet<String>,
    out: &mut BTreeMap<(String, String, String), Vec<Row>>,
) -> Result<()> {
    let schema = batch.schema();
    let col = |name: &str| -> Result<&Arc<dyn Array>> {
        match schema.index_of(name) {
            Ok(i) => Ok(batch.column(i)),
            Err(_) => Err(anyhow!("column {} missing", name)),
        }
    };
    let ts_arr = col("ts")?
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| anyhow!("ts not timestamp[us]"))?;
    let end_ts_arr = col("end_ts")?
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| anyhow!("end_ts not timestamp[us]"))?;
    let source_arr = col("source")?
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| anyhow!("source not int32"))?;
    let symbol_arr = col("symbol")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("symbol not string"))?;
    let exchange_arr = col("exchange")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("exchange not string"))?;
    let freq_arr = col("freq")?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow!("freq not string"))?;

    // DECIMAL(38,18) → f64，与 SQL 层 CAST(o AS DOUBLE) 同语义
    let to_f64 = |a: &Arc<dyn Array>| -> Result<Float64Array> {
        let casted = cast(a, &DataType::Float64)
            .map_err(|e| anyhow!("decimal cast: {e}"))?;
        let f = casted
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow!("cast produced non-f64"))?
            .clone();
        Ok(f)
    };
    let o_arr = to_f64(col("o")?)?;
    let c_arr = to_f64(col("c")?)?;
    let h_arr = to_f64(col("h")?)?;
    let l_arr = to_f64(col("l")?)?;
    let v_arr = col("v")?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow!("v not f64"))?
        .clone();
    let qv_arr = col("qv")?
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow!("qv not f64"))?
        .clone();

    let n = batch.num_rows();
    let mut mask = Vec::with_capacity(n);
    for i in 0..n {
        let ts_ms = ts_arr.value(i) / 1000;
        let keep = ts_ms >= start
            && ts_ms <= end
            && source_arr.value(i) != 1
            && symbols.contains(symbol_arr.value(i));
        mask.push(keep);
    }
    let mask = BooleanArray::from(mask);
    if mask.false_count() == n {
        return Ok(());
    }

    for i in 0..n {
        if !mask.value(i) {
            continue;
        }
        let key = (
            exchange_arr.value(i).to_string(),
            symbol_arr.value(i).to_string(),
            freq_arr.value(i).to_string(),
        );
        out.entry(key).or_default().push(Row {
            ts: ts_arr.value(i) / 1000,
            open: o_arr.value(i),
            close: c_arr.value(i),
            high: h_arr.value(i),
            low: l_arr.value(i),
            vol: v_arr.value(i),
            qv: qv_arr.value(i),
            end_ts: end_ts_arr.value(i) / 1000,
        });
    }
    Ok(())
}
