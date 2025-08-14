use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::{Method, Request, Response};
use hyper_util::client::legacy::Client;
use hyperlocal::{UnixConnector, Uri};
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug)]
pub enum LoadBalancerError {
    ConnectionFailed,
    WriteError,
    ReadError,
    Timeout,
    PoolExhausted,
    NoHealthyBackends,
}

pub struct UnixLoadBalancerConfig {
    pub backends: Vec<String>,
}

impl UnixLoadBalancerConfig {
    pub fn from_env() -> Self {
        UnixLoadBalancerConfig {
            backends: std::env::var("BACKENDS")
                .unwrap_or_else(|_| "unix:///tmp/backend.sock".to_string())
                .split(',')
                .map(|s| s.to_string())
                .collect(),
        }
    }
}

pub struct UnixLoadBalancer {
    current_index: AtomicUsize,
    backends: Vec<String>,
    client: Client<UnixConnector, Incoming>,
    backend_count: usize,
}

impl UnixLoadBalancer {
    pub fn new(config: UnixLoadBalancerConfig) -> Self {
        let connector = UnixConnector;
        let client = Client::builder(hyper_util::rt::TokioExecutor::new())
            .pool_max_idle_per_host(2048)
            .pool_idle_timeout(std::time::Duration::from_secs(2))
            .http1_max_buf_size(16 * 1024)
            .http1_writev(true)
            .http1_preserve_header_case(false)
            .http1_title_case_headers(false)
            .pool_timer(hyper_util::rt::TokioTimer::new())
            .build(connector);

        UnixLoadBalancer {
            current_index: AtomicUsize::new(0),
            client,
            backend_count: config.backends.len(),
            backends: config.backends,
        }
    }

    pub async fn forward_request(
        &self,
        method: Method,
        original_uri: hyper::Uri,
        body: Incoming,
    ) -> Result<Response<Incoming>, LoadBalancerError> {
        let backend = self.select_backend()?;

        let path_and_query = original_uri
            .path_and_query()
            .map(|pq| pq.as_str())
            .unwrap_or("/");

        let uri = Uri::new(backend, path_and_query);

        let request = Request::builder()
            .method(method)
            .uri(uri)
            .body(body)
            .map_err(|_| LoadBalancerError::WriteError)?;

        let response = self
            .client
            .request(request)
            .await
            .map_err(|_| LoadBalancerError::ConnectionFailed)?;

        Ok(response)
    }

    #[inline(always)]
    fn select_backend(&self) -> Result<&str, LoadBalancerError> {
        if self.backends.is_empty() {
            return Err(LoadBalancerError::NoHealthyBackends);
        }

        let index = self.current_index.fetch_add(1, Ordering::Relaxed) % self.backend_count;
        Ok(&self.backends[index].as_str())
    }
}
