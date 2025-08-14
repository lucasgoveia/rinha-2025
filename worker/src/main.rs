mod payment_message;
mod receiver;
mod worker_pool;
mod health_monitor;
mod processor_type;
mod payment_processor;
mod payment;
mod store;

use crate::receiver::Receiver;
use std::sync::Arc;
use deadpool_postgres::{Manager, ManagerConfig, RecyclingMethod};
use tokio_postgres::NoTls;
use crate::health_monitor::HealthMonitor;

pub struct WorkerConfig {
    pub listen_path: String,
    pub num_workers: usize,
    pub postgres_url: String,
    pub default_processor_url: String,
    pub fallback_processor_url: String,
}

impl WorkerConfig {
    pub fn from_env() -> WorkerConfig {
        let listen_path = std::env::var("LISTEN_PATH").unwrap();
        let num_workers = std::env::var("NUM_WORKERS").unwrap();
        let postgres_url = std::env::var("POSTGRES_URL").unwrap();
        let default_processor_url = std::env::var("DEFAULT_PROCESSOR_URL").unwrap();
        let fallback_processor_url = std::env::var("FALLBACK_PROCESSOR_URL").unwrap();

        WorkerConfig {
            listen_path,
            num_workers: usize::from_str_radix(&num_workers, 10).unwrap(),
            postgres_url,
            default_processor_url,
            fallback_processor_url,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize tracing with default level WARN, overridable via RUST_LOG
    {
        use tracing_subscriber::{EnvFilter, fmt};
        let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
        let _ = fmt().with_env_filter(env_filter).try_init();
    }

    let config = WorkerConfig::from_env();

    let pg_config = config.postgres_url
        .parse::<tokio_postgres::Config>()
        .expect("Invalid DATABASE_URL");

    let mgr = Manager::from_config(
        pg_config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast
        }
    );

    let pool = deadpool_postgres::Pool::builder(mgr)
        .max_size(config.num_workers)
        .build()
        .unwrap();

    let health_monitor = HealthMonitor::new(
        config.default_processor_url.clone().as_str(),
        config.default_processor_url.clone().as_str()
    );
    health_monitor.start().await;
    let health_monitor = Arc::new(health_monitor);

    let default_processor = Arc::new(payment_processor::PaymentProcessor::new(config.default_processor_url.clone()));
    let fallback_processor = Arc::new(payment_processor::PaymentProcessor::new(config.fallback_processor_url.clone()));

    let mut store = store::Store::new(pool);
    store.init().await;
    let store = Arc::new(store);

    let mut worker_pool = worker_pool::WorkerPool::new(config.num_workers, health_monitor, default_processor, fallback_processor, store);
    worker_pool.start().await;
    let worker_pool = Arc::new(worker_pool);

    let mut receiver = Receiver::new(config.listen_path, worker_pool);

    Ok(receiver.start().await?)
}
