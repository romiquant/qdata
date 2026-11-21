//! ClickHouse 读后端：原生 Arrow Flight SQL（arrowflight_port），与
//! TDEngine::query_kline 同契约。SQL 进、Arrow RecordBatch 出，零行协议转换。
//!
//! 表结构（与 TDengine 重建库语义一致）：
//!   kline(ts DateTime64(3), exchange LowCardinality(String), symbol String,
//!         freq LowCardinality(String), o,c,h,l,v,qv Float64,
//!         end_ts Nullable(DateTime64(3))) MergeTree PARTITION BY toYYYYMM(ts)
//!         ORDER BY (exchange, freq, symbol, ts)
//!   factor.factor(ts, exchange, symbol, fq, f) —— 小表
//!
//! 环境变量：CLICKHOUSE_FLIGHT_URL（如 grpc+tcp://127.0.0.1:8125）、
//!           CLICKHOUSE_USER / CLICKHOUSE_PASSWORD

use crate::backend::{
    f64_col_by_name, i64_col, send_grouped_series_from_batches, string_col, tag_sets, FactorCache,
    KlineBackend,
};
use crate::flight_service::DatasetTicket;
use crate::kline_processor::{FactorRecord, KlineSeries};
use anyhow::{anyhow, Result};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::{FlightDescriptor, flight_service_client::FlightServiceClient};
use async_trait::async_trait;
use futures::TryStreamExt;
use std::str::FromStr;
use tokio::sync::mpsc;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::Request;

pub struct ClickHouseBackend {
    client: FlightServiceClient<Channel>,
    auth_header: String,
    factor_db: String,
    factor_cache: FactorCache,
}

/// 标准 base64（避免为一个小函数引入依赖）。
fn base64_encode(data: &[u8]) -> String {
    const T: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { T[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { T[n as usize & 63] as char } else { '=' });
    }
    out
}

fn auth_req<T>(req: T, auth_header: &str) -> Result<Request<T>> {
    let mut r = Request::new(req);
    let v = MetadataValue::from_str(auth_header)
        .map_err(|e| anyhow!("bad auth header: {e}"))?;
    r.metadata_mut().insert("authorization", v);
    Ok(r)
}

impl ClickHouseBackend {
    pub async fn connect(url: &str, user: &str, password: &str, factor_db: String) -> Result<Self> {
        let addr = url
            .strip_prefix("grpc+tcp://")
            .or_else(|| url.strip_prefix("http://"))
            .unwrap_or(url)
            .trim_end_matches('/')
            .to_string();
        let channel = Channel::from_shared(format!("http://{addr}"))
            .map_err(|e| anyhow!("bad flight url {addr}: {e}"))?
            .connect()
            .await
            .map_err(|e| anyhow!("connect ClickHouse flight {addr}: {e}"))?;
        let auth_header = format!("Basic {}", base64_encode(format!("{user}:{password}").as_bytes()));
        let backend = Self {
            // CH 单分区批次可远超 tonic 默认 4MB 解码上限，放宽到 256MB
            client: FlightServiceClient::new(channel).max_decoding_message_size(256 * 1024 * 1024),
            auth_header,
            factor_db,
            factor_cache: FactorCache::new(),
        };
        backend.cache_factors().await?;
        Ok(backend)
    }

    async fn run_query(&self, sql: String) -> Result<Vec<RecordBatch>> {
        let mut client = self.client.clone();
        let info = client
            .get_flight_info(auth_req(
                FlightDescriptor::new_cmd(sql.into_bytes()),
                &self.auth_header,
            )?)
            .await
            .map_err(|e| anyhow!("get_flight_info: {e}"))?
            .into_inner();
        // CH 会把结果按分区拆成多个 endpoint（每个各持一个 ticket），
        // 只读 endpoint[0] 会截断到单分区行数（如 24576/62988），必须遍历全部。
        let mut batches = Vec::new();
        for ep in &info.endpoint {
            let ticket = ep
                .ticket
                .clone()
                .ok_or_else(|| anyhow!("flight info endpoint without ticket"))?;
            let stream = client
                .do_get(auth_req(ticket, &self.auth_header)?)
                .await
                .map_err(|e| anyhow!("do_get: {e}"))?
                .into_inner();
            let part = FlightRecordBatchStream::new_from_flight_data(
                stream.map_err(|e| arrow_flight::error::FlightError::DecodeError(e.to_string())),
            )
            .try_collect::<Vec<RecordBatch>>()
            .await
            .map_err(|e| anyhow!("decode flight data: {e}"))?;
            batches.extend(part);
        }
        Ok(batches)
    }
}

#[async_trait]
impl KlineBackend for ClickHouseBackend {
    async fn cache_factors(&self) -> Result<()> {
        let sql = format!(
            "SELECT exchange, symbol, ts, f FROM {}.factor WHERE fq='qfq'",
            self.factor_db
        );
        let batches = self.run_query(sql).await?;
        let mut records = Vec::new();
        for b in &batches {
            let ex = string_col(b, "exchange")?;
            let sym = string_col(b, "symbol")?;
            let ts = i64_col(b, "ts")?;
            let f = f64_col_by_name(b, "f")?;
            for j in 0..b.num_rows() {
                records.push(FactorRecord {
                    exchange: ex.value(j).to_string(),
                    symbol: sym.value(j).to_string(),
                    ts: ts.value(j),
                    f: f.value(j),
                });
            }
        }
        self.factor_cache.store(records).await;
        Ok(())
    }

    async fn get_cached_factors(
        &self,
        exchange: &str,
        symbol: &str,
    ) -> Result<Vec<FactorRecord>> {
        self.factor_cache.get(exchange, symbol).await
    }

    async fn query_kline(
        &self,
        ticket: &DatasetTicket,
        tx: mpsc::Sender<Result<KlineSeries>>,
    ) -> Result<()> {
        let (exchanges, symbols, freqs) = tag_sets(ticket)?;
        // ticket.start_time/end_time 已是 epoch ms，直接喂 fromUnixTimestamp64Milli
        // （此前误乘 1000 变 µs，窗口落在数万年后恒为空集）
        let start_ms = ticket.start_time;
        let end_ms = ticket.end_time; // 闭区间语义
        // 方案 B：kline 表入库时已预过滤 source!=1（与 TDengine 查询语义一致），
        // 这里不再过滤；raw 语义仅在 qdata 出口处决定是否做 qfq 复权。
        let sql = format!(
            "SELECT exchange, symbol, freq, ts, o, c, h, l, v, qv, end_ts \
             FROM kline FINAL \
             WHERE ts >= fromUnixTimestamp64Milli({start_ms}) \
               AND ts <= fromUnixTimestamp64Milli({end_ms}) \
               AND exchange IN ({}) AND symbol IN ({}) AND freq IN ({}) \
             ORDER BY exchange, symbol, freq, ts",
            exchanges.join(","),
            symbols.join(","),
            freqs.join(","),
        );
        let batches = self.run_query(sql).await?;
        if batches.is_empty() {
            // 与 TDengine 语义一致：无数据时发 fallback 空 series（如 em 板块空组）
            if let Some(tag) = ticket.tags.first() {
                if let Ok((e, s, f)) = crate::flight_service::DatasetTicket::parse_tag(tag) {
                    let _ = tx.send(Ok(KlineSeries::new(e.to_string(), s.to_string(), f.to_string()))).await;
                }
            }
            return Ok(());
        }
        send_grouped_series_from_batches(batches, &tx, ticket.tags.first().map(|s| s.as_str()))
            .await
    }
}
