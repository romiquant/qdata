//! K 线存储后端抽象：TDengine / QuestDB / ClickHouse 三种实现共用同一契约。
//!
//! 契约（与 TDEngine::query_kline 语义逐字一致）：
//! - [start, end] 闭区间、`source != 1` 过滤
//! - 结果按 (exchange, symbol, freq) 字典序分组、组内 ts 升序
//! - 通过 tx 逐组发送 KlineSeries；无数据时发送一个空 series 保证
//!   Flight 流含 schema 消息
//!
//! 由 `KLINE_BACKEND` 环境变量选择：`tdengine`（默认）/ `questdb` / `clickhouse`。
//! `iceberg` 分支独立于本 trait（直读 parquet 文件，见 iceberg_store）。

use crate::flight_service::DatasetTicket;
use crate::kline_processor::{FactorRecord, KlineSeries};
use anyhow::{anyhow, Result};
use arrow::array::{
    Array, Decimal128Array, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::SystemTime;
use tokio::sync::{mpsc, Mutex};

#[async_trait]
pub trait KlineBackend: Send + Sync {
    /// 加载 qfq 复权因子缓存（两端口共用同一张 factor 表语义）。
    async fn cache_factors(&self) -> Result<()>;

    async fn get_cached_factors(
        &self,
        exchange: &str,
        symbol: &str,
    ) -> Result<Vec<FactorRecord>>;

    async fn query_kline(&self, ticket: &DatasetTicket, tx: mpsc::Sender<Result<KlineSeries>>)
    -> Result<()>;

    /// do_put 演示路径用的原始 SQL 执行（仅 TDengine 实现有效）。
    async fn execute_sql_file(&self, path: &str) -> Result<()> {
        let _ = path;
        Err(anyhow!("execute_sql_file not supported by this backend"))
    }
}

/// 后端共享的因子缓存（内存结构 + 读写锁），各实现只负责拉数据。
pub struct FactorCache {
    cache: Mutex<Option<(SystemTime, HashMap<String, Vec<FactorRecord>>)>>,
}

impl FactorCache {
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(None),
        }
    }

    fn key(exchange: &str, symbol: &str) -> String {
        format!("{exchange}_{symbol}")
    }

    pub async fn store(&self, records: Vec<FactorRecord>) {
        let mut map: HashMap<String, Vec<FactorRecord>> = HashMap::new();
        for f in records {
            map.entry(Self::key(&f.exchange, &f.symbol))
                .or_default()
                .push(f);
        }
        for v in map.values_mut() {
            v.sort_by_key(|f| f.ts);
        }
        let n = map.len();
        *self.cache.lock().await = Some((SystemTime::now(), map));
        tracing::info!("Factor cache loaded: {n} exchange_symbol pairs");
    }

    pub async fn get(&self, exchange: &str, symbol: &str) -> Result<Vec<FactorRecord>> {
        let cache = self.cache.lock().await;
        if let Some((_, map)) = cache.as_ref() {
            Ok(map.get(&Self::key(exchange, symbol)).cloned().unwrap_or_default())
        } else {
            Ok(vec![])
        }
    }
}

/// 后端共享的"已排序行 → 分组 KlineSeries 流"发送逻辑。
/// 输入必须已按 (exchange, symbol, freq, ts) 排序（SQL ORDER BY 保证）。
pub async fn send_grouped_series_from_batches(
    batches: Vec<RecordBatch>,
    tx: &mpsc::Sender<Result<KlineSeries>>,
    fallback_tag: Option<&str>,
) -> Result<()> {
    // 按列名取列，容忍列序变化
    let first = batches.first().ok_or_else(|| anyhow!("no batches"))?;
    let schema = first.schema();
    let idx = |name: &str| -> Result<usize> {
        schema
            .index_of(name)
            .map_err(|_| anyhow!("column {name} missing"))
    };
    let (i_ex, i_sym, i_fr, i_ts) = (idx("exchange")?, idx("symbol")?, idx("freq")?, idx("ts")?);
    let (i_o, i_c, i_h, i_l) = (idx("o")?, idx("c")?, idx("h")?, idx("l")?);
    let (i_v, i_qv, i_end) = (idx("v")?, idx("qv")?, idx("end_ts")?);
    // 先按首批次校验列可读性（延迟到首个批次时提取，见循环内）

    let f64_col = |b: &RecordBatch, i: usize| -> Result<Float64Array> {
        // o/c/h/l 可能是 Decimal128(38,18)（TDengine 重建库）或 Float64，统一转 f64
        let arr = b.column(i);
        if let Some(d) = arr.as_any().downcast_ref::<Decimal128Array>() {
            let mut vals = Vec::with_capacity(d.len());
            for j in 0..d.len() {
                vals.push(if d.is_null(j) {
                    f64::NAN
                } else {
                    // scale 18 → f64
                    d.value(j) as f64 / 10f64.powi(18)
                });
            }
            return Ok(Float64Array::from(vals));
        }
        let f = arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow!("column {} not f64/decimal", schema.field(i).name()))?;
        Ok(f.clone())
    };

    let mut current = (String::new(), String::new(), String::new());
    let mut series: Option<KlineSeries> = None;
    let mut any_sent = false;

    // ts/end_ts 兼容 timestamp[ms/us/ns]（CH DateTime64(3)=ms、iceberg parquet=us）
    // 与裸 Int64 ms（缓存路径），统一归一到 ms
    let ts_col_ms = |b: &RecordBatch, i: usize, name: &str| -> Result<Vec<i64>> {
        use arrow::array::{Int64Array, TimestampMillisecondArray, TimestampNanosecondArray};
        let arr = b.column(i);
        let (vals, unit): (Vec<i64>, i64) =
            if let Some(a) = arr.as_any().downcast_ref::<TimestampMillisecondArray>() {
                (a.iter().map(|v| v.unwrap_or(0)).collect(), 1)
            } else if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                (a.iter().map(|v| v.unwrap_or(0)).collect(), 1000)
            } else if let Some(a) = arr.as_any().downcast_ref::<TimestampNanosecondArray>() {
                (a.iter().map(|v| v.unwrap_or(0)).collect(), 1_000_000)
            } else if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                (a.iter().map(|v| v.unwrap_or(0)).collect(), 1)
            } else {
                return Err(anyhow!("{name} not timestamp[ms/us/ns]/i64"));
            };
        Ok(vals.into_iter().map(|v| v / unit).collect())
    };

    for b in &batches {
        let ex = b
            .column(i_ex)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("exchange not string"))?
            .clone();
        let sym = b
            .column(i_sym)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("symbol not string"))?
            .clone();
        let fr = b
            .column(i_fr)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("freq not string"))?
            .clone();
        let ms_ts = ts_col_ms(b, i_ts, "ts")?;
        let o = f64_col(b, i_o)?;
        let c = f64_col(b, i_c)?;
        let h = f64_col(b, i_h)?;
        let l = f64_col(b, i_l)?;
        let v = b
            .column(i_v)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow!("v not f64"))?
            .clone();
        let qv = b
            .column(i_qv)
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| anyhow!("qv not f64"))?
            .clone();
        let ms_end = ts_col_ms(b, i_end, "end_ts")?;

        for j in 0..b.num_rows() {
            let key = (
                ex.value(j).to_string(),
                sym.value(j).to_string(),
                fr.value(j).to_string(),
            );
            if key != current || series.is_none() {
                if let Some(s) = series.take() {
                    if tx.send(Ok(s)).await.is_err() {
                        tracing::warn!("Receiver dropped, stopping query");
                        return Ok(());
                    }
                    any_sent = true;
                }
                current = key.clone();
                series = Some(KlineSeries::new(key.0, key.1, key.2));
            }
            let ts_ms = ms_ts[j];
            if let Some(ref mut s) = series {
                s.push(
                    ts_ms,
                    o.value(j),
                    c.value(j),
                    h.value(j),
                    l.value(j),
                    v.value(j),
                    qv.value(j),
                    ms_end[j],
                );
            }
        }
    }

    match series {
        Some(s) => {
            let _ = tx.send(Ok(s)).await;
        }
        None if !any_sent => {
            tracing::info!("No data found for query, returning empty series");
            if let Some(tag) = fallback_tag {
                if let Ok((e, s, f)) = DatasetTicket::parse_tag(tag) {
                    let _ = tx
                        .send(Ok(KlineSeries::new(e.to_string(), s.to_string(), f.to_string())))
                        .await;
                }
            }
        }
        None => {}
    }
    Ok(())
}

/// 后端共享的 tag → (exchanges, symbols, freqs) IN 列表提取。
pub fn tag_sets(ticket: &DatasetTicket) -> Result<(Vec<String>, Vec<String>, Vec<String>)> {
    let mut exchanges = Vec::new();
    let mut symbols = Vec::new();
    let mut freqs = Vec::new();
    for tag in &ticket.tags {
        let (e, s, f) = DatasetTicket::parse_tag(tag)?;
        exchanges.push(format!("'{e}'"));
        symbols.push(format!("'{s}'"));
        freqs.push(format!("'{f}'"));
    }
    Ok((exchanges, symbols, freqs))
}

/// 后端共享的 Int64Array 列提取（factor 加载用，返回 epoch ms）。
pub fn i64_col(b: &RecordBatch, name: &str) -> Result<Int64Array> {
    use arrow::datatypes::{DataType, TimeUnit};
    let i = b
        .schema()
        .index_of(name)
        .map_err(|_| anyhow!("column {name} missing"))?;
    let arr = b.column(i);
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.clone());
    }
    // 各精度 timestamp 统一归一到 ms
    let unit_div = match arr.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => 1_000i64,
        DataType::Timestamp(TimeUnit::Millisecond, _) => 1,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1_000_000,
        DataType::Date32 => return Err(anyhow!("date32 unsupported for {name}")),
        other => return Err(anyhow!("column {name} not i64/timestamp: {other:?}")),
    };
    let vals: Vec<i64> = (0..arr.len())
        .map(|j| {
            let v = if let Some(a) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                a.value(j)
            } else if let Some(a) = arr.as_any().downcast_ref::<arrow::array::TimestampMillisecondArray>() {
                a.value(j)
            } else if let Some(a) = arr.as_any().downcast_ref::<arrow::array::TimestampSecondArray>() {
                a.value(j)
            } else if let Some(a) = arr.as_any().downcast_ref::<arrow::array::TimestampNanosecondArray>() {
                a.value(j)
            } else {
                0
            };
            v / unit_div
        })
        .collect();
    Ok(Int64Array::from(vals))
}

pub fn string_col(b: &RecordBatch, name: &str) -> Result<StringArray> {
    let i = b
        .schema()
        .index_of(name)
        .map_err(|_| anyhow!("column {name} missing"))?;
    b.column(i)
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| anyhow!("column {name} not string"))
}

/// f64 列提取（Decimal128(38,18) 与 Float64 都接受）。
pub fn f64_col_by_name(b: &RecordBatch, name: &str) -> Result<Float64Array> {
    let i = b
        .schema()
        .index_of(name)
        .map_err(|_| anyhow!("column {name} missing"))?;
    let arr = b.column(i);
    if let Some(f) = arr.as_any().downcast_ref::<Float64Array>() {
        return Ok(f.clone());
    }
    if let Some(d) = arr.as_any().downcast_ref::<Decimal128Array>() {
        let vals: Vec<f64> = (0..d.len()).map(|j| d.value(j) as f64 / 10f64.powi(18)).collect();
        return Ok(Float64Array::from(vals));
    }
    Err(anyhow!("column {name} not numeric"))
}

/// 发送结果或告警（接收端关闭时停止查询）。
pub async fn send_or_warn(
    tx: &mpsc::Sender<Result<KlineSeries>>,
    result: Result<KlineSeries>,
) -> bool {
    if tx.send(result).await.is_err() {
        tracing::warn!("Receiver dropped, stopping query");
        return false;
    }
    true
}
