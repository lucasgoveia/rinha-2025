use crate::publisher::Publisher;
use std::env;
use deadpool_postgres::{Manager, ManagerConfig, RecyclingMethod};
use tokio_postgres::NoTls;

#[derive(Clone)]
pub struct GatewayConfig {
    pub publish_path: String,
    pub listen_path: String,
    pub postgres_url: String,
}

impl GatewayConfig {
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listen_path = env::var("GATEWAY_LISTEN_SOCKET").unwrap();

        let publish_path = env::var("GATEWAY_PUBLISH_SOCKET").unwrap();

        let postgres_url = env::var("POSTGRES_URL").unwrap();

        Ok(Self {
            listen_path,
            publish_path,
            postgres_url
        })
    }
}

pub struct Gateway {
    pub publisher: Publisher,
    pub pool: deadpool_postgres::Pool,
}

impl Gateway {
    pub async fn new(
        config: GatewayConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let publisher = Publisher::new(config.publish_path, 1024).await?;

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
            .max_size(3)
            .runtime(deadpool_postgres::Runtime::Tokio1)
            .build()
            .unwrap();

        Ok(Self { publisher, pool })
    }
}
