//! QuestDB 读后端：PG wire 协议（tokio-postgres），与 TDEngine::query_kline 同契约。
//!
//! 表结构（见 ops/dep 侧建表 SQL）：
//!   kline(ts TIMESTAMP, exchange SYMBOL, symbol SYMBOL, freq SYMBOL,
//!         o,h,l,c,v,qv, end_ts) PARTITION BY MONTH WAL
//!   factor.factor(factor_ts TIMESTAMP, exchange, symbol, fq, f) —— 小表
//!
//! 环境变量：QUESTDB_DSN（postgres://user:pass@host:8812/qdb）

use crate::backend::{tag_sets, FactorCache, KlineBackend};
use crate::flight_service::DatasetTicket;
use crate::kline_processor::{FactorRecord, KlineSeries};
use anyhow::{anyhow, Result};
use async_trait::async_trait;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

pub struct QuestDbBackend {
    pool: Pool,
    factor_db: String,
    factor_cache: FactorCache,
}

/// QuestDB 存的 ts 为 TIMESTAMP（微秒 epoch）；SQL 侧用 epoch 字符串比较。
fn us_to_iso(us: i64) -> String {
    chrono::DateTime::from_timestamp(us.div_euclid(1_000_000), (us.rem_euclid(1_000_000)) as u32 * 1000)
        .map(|dt| dt.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .unwrap_or_default()
}

impl QuestDbBackend {
    pub async fn connect(dsn: &str, _kline_db: String, factor_db: String) -> Result<Self> {
        let mgr = Manager::from_config(
            dsn.parse::<tokio_postgres::Config>().map_err(|e| anyhow!("bad QUESTDB_DSN: {e}"))?,
            NoTls,
            ManagerConfig { recycling_method: RecyclingMethod::Fast },
        );
        let pool = Pool::builder(mgr).max_size(10).build()?;
        let backend = Self {
            pool,
            factor_db,
            factor_cache: FactorCache::new(),
        };
        backend.cache_factors().await?;
        Ok(backend)
    }

    async fn fetch_factor_records(&self) -> Result<Vec<FactorRecord>> {
        let client = self.pool.get().await.map_err(|e| anyhow!("pool: {e:#}"))?;
        // QuestDB 无库名概念（PG wire 单库），factor 表用裸名
        let _ = &self.factor_db;
        let sql = "SELECT exchange, symbol, cast(ts as long) as ts, f FROM factor WHERE fq = 'qfq'";
        let rows = client.query(sql, &[]).await.map_err(|e| anyhow!("factor query: {e:#}"))?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let exchange: String = r.try_get(0)?;
            let symbol: String = r.try_get(1)?;
            // ts 可能以 TIMESTAMP（us）或 BIGINT（ms）到达，二者都接受
            // cast(ts as long) 返回微秒 epoch → 归一到 ms（与 TDengine 因子表一致）
            let ts_us: i64 = r.try_get(2)?;
            let ts = ts_us / 1000;
            let f: f64 = r.try_get(3)?;
            out.push(FactorRecord { exchange, symbol, ts, f });
        }
        Ok(out)
    }
}

#[async_trait]
impl KlineBackend for QuestDbBackend {
    async fn cache_factors(&self) -> Result<()> {
        let records = self.fetch_factor_records().await?;
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
        // 方案 B：kline 表入库时已预过滤 source!=1（与 TDengine 查询语义一致），
        // 这里不再过滤；raw 语义仅在 qdata 出口处决定是否做 qfq 复权。
        let sql = format!(
            "SELECT exchange, symbol, freq, cast(ts as long) as ts, o, c, h, l, v, qv, cast(end_ts as long) as end_ts \
             FROM kline \
             WHERE ts >= '{}' AND ts <= '{}' \
             AND exchange IN ({}) AND symbol IN ({}) AND freq IN ({}) \
             ORDER BY exchange, symbol, freq, ts",
            us_to_iso(ticket.start_time * 1000),
            us_to_iso(ticket.end_time * 1000),
            exchanges.join(","),
            symbols.join(","),
            freqs.join(","),
        );

        let client = self.pool.get().await.map_err(|e| anyhow!("pool: {e:#}"))?;
        let stmt = client.prepare(&sql).await.map_err(|e| anyhow!("prepare: {e:#}"))?;
        let rows = client.query(&stmt, &[]).await.map_err(|e| anyhow!("kline query: {e:#}"))?;

        let mut current = (String::new(), String::new(), String::new());
        let mut series: Option<KlineSeries> = None;
        let mut any_sent = false;
        for r in rows {
            let exchange: String = r.try_get(0)?;
            let symbol: String = r.try_get(1)?;
            let freq: String = r.try_get(2)?;
            let ts_us: i64 = r.try_get(3)?;
            let o: f64 = r.try_get(4)?;
            let c: f64 = r.try_get(5)?;
            let h: f64 = r.try_get(6)?;
            let l: f64 = r.try_get(7)?;
            let v: f64 = r.try_get(8)?;
            let qv: f64 = r.try_get(9)?;
            let end_us: i64 = r.try_get(10)?;

            let key = (exchange, symbol, freq);
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
            if let Some(ref mut s) = series {
                s.push(ts_us / 1000, o, c, h, l, v, qv, end_us / 1000);
            }
        }
        match series {
            Some(s) => {
                let _ = tx.send(Ok(s)).await;
            }
            None if !any_sent => {
                tracing::info!("No data found for query, returning empty series");
                if let Some(tag) = ticket.tags.first() {
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
}
