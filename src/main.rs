use arrow_flight::flight_service_server::FlightServiceServer;
use std::env;
use std::sync::Arc;
use tonic::transport::Server;
use tracing::{error, info, warn};

mod backend;
mod clickhouse_backend;
mod db;
mod flight_error;
mod flight_service;
mod iceberg_store;
mod kline_processor;
mod redis_client;

use db::TDEngine;
use flight_service::KlineFlightService;
use redis_client::init_redis;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO) // 设置日志级别
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    info!("Application starting");

    // DSN 仅指向 TDengine 实例（host/port/auth），不带库名。
    // 库名由 KLINE_DB / FACTOR_DB 单独从 env 注入，避免两处重复。
    let (dsn, kline_db, factor_db) = match (
        env::var("TAOS_DSN").ok().filter(|s| !s.is_empty()),
        env::var("KLINE_DB").ok().filter(|s| !s.is_empty()),
        env::var("FACTOR_DB").ok().filter(|s| !s.is_empty()),
    ) {
        (Some(d), Some(k), Some(f)) => (d, k, f),
        _ => {
            error!("TAOS_DSN, KLINE_DB and FACTOR_DB are required (e.g. TAOS_DSN=taos://root:taosdata@localhost:6030, KLINE_DB=kline, FACTOR_DB=factor)");
            std::process::exit(1);
        }
    };

    let addr = env::var("FLIGHT_ADDR").unwrap_or_else(|_| "0.0.0.0:50001".to_string());
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1/".to_string());

    // Initialize Redis
    if let Err(e) = init_redis(&redis_url).await {
        error!("Failed to initialize Redis at {}: {}", redis_url, e);
        warn!("Redis initialization failed; service continues without Redis cache");
    } else {
        info!("Redis initialized: {}", redis_url);
    }

    // 按 KLINE_BACKEND 选择读后端：tdengine（默认）/ clickhouse。
    // iceberg 分支（直读 parquet 文件）仍叠加在后端之上，见下方 match。
    let backend_name = env::var("KLINE_BACKEND").unwrap_or_default();
    let db: Arc<dyn crate::backend::KlineBackend> = match backend_name.as_str() {
        "clickhouse" => {
            let url = env::var("CLICKHOUSE_FLIGHT_URL").unwrap_or_else(|_| {
                error!("KLINE_BACKEND=clickhouse requires CLICKHOUSE_FLIGHT_URL (grpc+tcp://host:8125)");
                std::process::exit(1);
            });
            let user = env::var("CLICKHOUSE_USER").unwrap_or_else(|_| "root".to_string());
            let pass = env::var("CLICKHOUSE_PASSWORD").unwrap_or_default();
            match clickhouse_backend::ClickHouseBackend::connect(&url, &user, &pass, factor_db.clone()).await {
                Ok(b) => {
                    info!("ClickHouse connected: flight={url} factor_db={factor_db}");
                    Arc::new(b)
                }
                Err(e) => {
                    error!("Failed to connect ClickHouse ({url}): {e}");
                    std::process::exit(1);
                }
            }
        }
        _ => match TDEngine::connect(&dsn, &kline_db, &factor_db).await {
            Ok(db) => {
                info!(
                    "TDengine connected: kline_db={} factor_db={} dsn={}",
                    kline_db, factor_db, dsn
                );
                Arc::new(db)
            }
            Err(e) => {
                error!(
                    "Failed to connect to TDengine (kline_db={} factor_db={} dsn={}): {}",
                    kline_db, factor_db, dsn, e
                );
                std::process::exit(1);
            }
        },
    };

    // KLINE_BACKEND=iceberg 时 kline 查询改走 Iceberg/Parquet warehouse，
    // factor 缓存与所选后端共用（clickhouse 分支不会进入此 match）。
    let iceberg = match backend_name.as_str() {
        "iceberg" => {
            let dir = env::var("ICEBERG_KLINE_DIR").unwrap_or_else(|_| {
                error!("KLINE_BACKEND=iceberg requires ICEBERG_KLINE_DIR");
                std::process::exit(1);
            });
            info!("kline backend = iceberg, data_root = {}", dir);
            Some(Arc::new(iceberg_store::IcebergStore::new(dir)))
        }
        _ => {
            info!("kline backend = tdengine");
            None
        }
    };

    // Start Flight service
    let service = KlineFlightService::new(db, iceberg);
    let svc = FlightServiceServer::new(service);
    let addr = addr.parse()?;
    info!("Starting Flight server on {}", addr);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
