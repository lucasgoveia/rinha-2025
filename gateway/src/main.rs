extern crate core;

mod gateway;
mod publisher;

use crate::gateway::{Gateway, GatewayConfig};
use deadpool_postgres::Pool;
use http_body_util::{combinators::BoxBody, BodyExt};
use http_body_util::{Empty, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;
use time::format_description::well_known::Rfc3339;
use time::PrimitiveDateTime;
use tokio::net::UnixListener;
use tokio_postgres::types::{FromSql, Type};

fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}
fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceType {
    Default,
    Fallback,
}

impl fmt::Display for ServiceType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServiceType::Default => write!(f, "default"),
            ServiceType::Fallback => write!(f, "fallback"),
        }
    }
}

impl<'a> FromSql<'a> for ServiceType {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        let s = std::str::from_utf8(raw)?;
        match s {
            "default" => Ok(ServiceType::Default),
            "fallback" => Ok(ServiceType::Fallback),
            _ => Err(format!("unknown service_type variant: {}", s).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "service_type"
    }
}


#[derive(Deserialize, Serialize)]
struct ProcessorSummary {
    #[serde(rename = "totalRequests")]
    total_requests: i64,
    #[serde(rename = "totalAmount")]
    total_amount: Decimal,
}

#[derive(Deserialize, Serialize)]
struct Summary {
    default: ProcessorSummary,
    fallback: ProcessorSummary,
}

async fn payments_summary_handler(
    pool: &Pool,
    from: Option<PrimitiveDateTime>,
    to: Option<PrimitiveDateTime>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    match pool.get().await {
        Ok(client) => {
            let stmt = client
                .prepare(
                    "
                SELECT COUNT(*) AS total_requests,
                      SUM(amount) AS total_amount,
                      service_used
                FROM payments
                WHERE ($1::timestamp IS NULL OR requested_at >= $1::timestamp)
                 AND ($2::timestamp IS NULL OR requested_at <= $2::timestamp)
                GROUP BY service_used;
            ",
                )
                .await
                .unwrap();

            let rows = client.query(&stmt, &[&from, &to]).await.unwrap();

            let mut default_summary = ProcessorSummary {
                total_requests: 0,
                total_amount: Decimal::ZERO
            };
            let mut fallback_summary = ProcessorSummary {
                total_requests: 0,
                total_amount: Decimal::ZERO,
            };

            for row in rows {
                let total_requests: i64 = row.get("total_requests");
                let total_amount: Decimal = row.get("total_amount");
                let processor: ServiceType = row.get("service_used");

                if processor == ServiceType::Default {
                    default_summary.total_requests = total_requests;
                    default_summary.total_amount = total_amount;
                } else {
                    fallback_summary.total_requests = total_requests;
                    fallback_summary.total_amount = total_amount;
                }
            }

            let summary = Summary {
                default: default_summary,
                fallback: fallback_summary,
            };

            let json_summary = serde_json::to_string(&summary).unwrap();
            let mut ok = Response::new(full(json_summary));
            *ok.status_mut() = hyper::StatusCode::OK;
            ok.headers_mut().insert(
                hyper::header::CONTENT_TYPE,
                "application/json".parse().unwrap(),
            );
            Ok(ok)
        }
        Err(_) => {
            let mut ok = Response::new(empty());
            *ok.status_mut() = hyper::StatusCode::INTERNAL_SERVER_ERROR;
            Ok(ok)
        }
    }
}

fn parse_query_params(req: &Request<Incoming>) -> HashMap<String, String> {
    let query = req.uri().query().unwrap_or("");
    form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect()
}

async fn echo(
    req: Request<Incoming>,
    gateway: Arc<Gateway>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/health") => Ok(Response::new(full("OK"))),
        (&Method::POST, "/payments") => {
            let body = req.into_body();
            let body_bytes = body.collect().await?.to_bytes();

            match gateway
                .publisher
                .publish(body_bytes.iter().as_slice())
                .await
            {
                Ok(_) => {
                    let mut ok = Response::new(empty());
                    *ok.status_mut() = hyper::StatusCode::ACCEPTED;
                    Ok(ok)
                }
                Err(_) => {
                    let mut ok = Response::new(empty());
                    *ok.status_mut() = hyper::StatusCode::TOO_MANY_REQUESTS;
                    Ok(ok)
                }
            }
            // let mut ok = Response::new(empty());
            // *ok.status_mut() = hyper::StatusCode::ACCEPTED;
            // Ok(ok)
        }
        (&Method::GET, "/payments-summary") => {
            let params = parse_query_params(&req);

            let from = params
                .get("from")
                .map(|s| PrimitiveDateTime::parse(s.as_str(), &Rfc3339).expect("Invalid date"));
            let to = params
                .get("to")
                .map(|s| PrimitiveDateTime::parse(s.as_str(), &Rfc3339).expect("Invalid date"));

            payments_summary_handler(&gateway.pool, from, to).await
        }
        (&Method::POST, "/purge-payments") => {
            match gateway.pool.get().await {
                Ok(client) => {
                    let stm = client.prepare("TRUNCATE TABLE payments").await.unwrap();

                    if let Err(_) = client.execute(&stm, &[]).await {
                        let mut ok = Response::new(empty());
                        *ok.status_mut() = hyper::StatusCode::INTERNAL_SERVER_ERROR;
                        return Ok(ok);
                    }

                    let mut ok = Response::new(empty());
                    *ok.status_mut() = hyper::StatusCode::OK;
                    Ok(ok)
                }
                Err(_) => {
                    let mut ok = Response::new(empty());
                    *ok.status_mut() = hyper::StatusCode::INTERNAL_SERVER_ERROR;
                    Ok(ok)
                }
            }
        }
        _ => {
            let mut not_found = Response::new(empty());
            *not_found.status_mut() = hyper::StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = GatewayConfig::from_env()?;
    let server = Arc::new(Gateway::new(config.clone()).await?);

    let socket_path = &config.listen_path;
    if std::fs::metadata(socket_path).is_ok() {
        std::fs::remove_file(socket_path)?;
    }

    let listener = UnixListener::bind(socket_path)?;

    let permissions = std::fs::Permissions::from_mode(0o666);
    std::fs::set_permissions(socket_path, permissions)?;

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await?;

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);
        let server_clone = Arc::clone(&server);

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            if let Err(err) = http1::Builder::new()
                .keep_alive(true)
                .half_close(false)
                .writev(true)
                .max_buf_size(16 * 1024)
                .preserve_header_case(false)
                .title_case_headers(false)
                .serve_connection(
                    io,
                    service_fn(move |req| echo(req, Arc::clone(&server_clone))),
                )
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
