use crate::processor_type::ProcessorType;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper;
use hyper::client::conn::http1;
use hyper::client::conn::http1::SendRequest;
use hyper::{Method, Request};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::interval;

#[derive(Debug, Deserialize)]
pub struct ProcessorHealth {
    pub failing: bool,
    #[serde(rename = "minResponseTime")]
    pub min_response_time: u16,
}

pub struct HealthMonitor {
    urls: HashMap<ProcessorType, String>,
    healths: Arc<RwLock<HashMap<ProcessorType, ProcessorHealth>>>,
}

const MAX_ACCEPTABLE_RESPONSE_TIME: u16 = 50;

#[derive(Debug)]
pub enum HealthMonitorError {
    BothProcessorsFailing,
}

impl std::fmt::Display for HealthMonitorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthMonitorError::BothProcessorsFailing => write!(f, "Both processors are failing"),
        }
    }
}

impl std::error::Error for HealthMonitorError {}
impl HealthMonitor {
    pub fn new(default_processor_url: &str, fallback_processor_url: &str) -> Self {
        let mut healths = HashMap::with_capacity(2);
        healths.insert(
            ProcessorType::Default,
            ProcessorHealth {
                min_response_time: 0,
                failing: false,
            },
        );
        healths.insert(
            ProcessorType::Fallback,
            ProcessorHealth {
                min_response_time: 0,
                failing: false,
            },
        );

        let mut urls = HashMap::with_capacity(2);
        urls.insert(ProcessorType::Default, default_processor_url.to_string());
        urls.insert(ProcessorType::Fallback, fallback_processor_url.to_string());

        Self {
            urls,
            healths: Arc::new(RwLock::new(healths)),
        }
    }

    pub async fn start(&self) {
        let client =
            Client::builder(hyper_util::rt::TokioExecutor::new()).build(HttpConnector::new());
        let default_url = self.urls.get(&ProcessorType::Default).unwrap().clone();
        let fallback_url = self.urls.get(&ProcessorType::Fallback).unwrap().clone();
        let healths = self.healths.clone();

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(5));

            loop {
                Self::try_update_health(&ProcessorType::Default, client.clone(), &default_url, healths.clone())
                    .await;
                Self::try_update_health(&ProcessorType::Fallback, client.clone(), &fallback_url, healths.clone())
                    .await;
                ticker.tick().await;
            }
        });
    }

    async fn try_update_health(processor_type: &ProcessorType, client: Client<HttpConnector, Empty<Bytes>>, url: &String, healths: Arc<RwLock<HashMap<ProcessorType, ProcessorHealth>>>) {
        match Self::probe_health(client, url).await {
            Ok(probed_health) => {
                let mut healths = healths.write().await;
                if let Some(health) = healths.get_mut(processor_type) {
                    health.failing = probed_health.failing;
                    health.min_response_time = probed_health.min_response_time;
                    tracing::info!(
                        processor = ?processor_type,
                        health = ?health,
                        "Updated health for processor"
                    );
                }
            }
            Err(err) => {
                tracing::warn!(error = ?err, "Failed to update health for processor");
            }
        }
    }

    // pub async fn next_processor(&self) -> Result<ProcessorType, HealthMonitorError> {
    //     let healths = self.healths.read().await;
    //     let default_health = healths.get(&ProcessorType::Default).unwrap();
    //     let fallback_health = healths.get(&ProcessorType::Fallback).unwrap();
    //
    //     let default_failing = default_health.failing
    //         || default_health.min_response_time > MAX_ACCEPTABLE_RESPONSE_TIME;
    //     let fallback_failing = fallback_health.failing
    //         || fallback_health.min_response_time > MAX_ACCEPTABLE_RESPONSE_TIME;
    //
    //     if default_failing && fallback_failing {
    //         return Err(HealthMonitorError::BothProcessorsFailing);
    //     }
    //
    //     if default_failing {
    //         return Ok(ProcessorType::Fallback);
    //     }
    //
    //     if fallback_failing {
    //         return Ok(ProcessorType::Default);
    //     }
    //
    //     if default_health.min_response_time < (3 * fallback_health.min_response_time) {
    //         return Ok(ProcessorType::Default);
    //     }
    //
    //     Ok(ProcessorType::Fallback)
    // }

    pub async fn next_processor(&self) -> Result<ProcessorType, HealthMonitorError> {
        let healths = self.healths.read().await;
        let default_health = healths.get(&ProcessorType::Default).unwrap();

        let default_failing = default_health.failing
            || default_health.min_response_time > MAX_ACCEPTABLE_RESPONSE_TIME;


        if default_failing  {
            return Err(HealthMonitorError::BothProcessorsFailing);
        }

        Ok(ProcessorType::Default)
    }

    async fn probe_health(
        client: Client<HttpConnector, Empty<Bytes>>,
        url: &str,
    ) -> Result<ProcessorHealth, Box<dyn std::error::Error + Send + Sync>> {
        let uri = format!("{}/payments/service-health", url).parse::<hyper::Uri>()?;

        let req = Request::builder()
            .uri(uri)
            .method(Method::GET)
            .body(Empty::<Bytes>::new())?;

        let res = client.request(req).await?;

        if res.status() != hyper::StatusCode::OK {
            return Err(format!("Invalid status code: {}", res.status()).into());
        }

        let body = res.into_body().collect().await?;
        let body = body.to_bytes();

        let health: ProcessorHealth = serde_json::from_slice(&body)?;

        Ok(health)
    }
}
