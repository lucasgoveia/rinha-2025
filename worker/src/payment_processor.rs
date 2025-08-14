use crate::payment::Payment;
use bytes::Bytes;
use http_body_util::Full;
use hyper::{Method, Request, StatusCode};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use rust_decimal::Decimal;
use serde::Serialize;
use time::OffsetDateTime;

pub struct PaymentProcessor {
    url: String,
    client: Client<HttpConnector, Full<Bytes>>,
}

#[derive(Debug)]
pub enum PaymentProcessorError {
    InvalidPayment,
    Unavailable,
}

impl std::fmt::Display for PaymentProcessorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PaymentProcessorError::Unavailable => write!(f, "processors is unavailable"),
            PaymentProcessorError::InvalidPayment => write!(f, "invalid payment"),
        }
    }
}

impl std::error::Error for PaymentProcessorError {}

#[derive(Debug, Serialize)]
struct PaymentRequest {
    pub amount: Decimal,
    #[serde(rename = "correlationId")]
    pub correlation_id: uuid::Uuid,
    #[serde(rename = "requestedAt", with = "time::serde::rfc3339")]
    pub requested_at: OffsetDateTime,
}

impl From<Payment> for PaymentRequest {
    fn from(p: Payment) -> Self {
        Self {
            amount: p.amount,
            correlation_id: p.correlation_id,
            requested_at: p.requested_at,
        }
    }
}

impl PaymentProcessor {
    pub fn new(url: String) -> Self {
        let client =
            Client::builder(hyper_util::rt::TokioExecutor::new()).build(HttpConnector::new());

        Self {
            url: format!("{}/payments", url),
            client,
        }
    }

    pub async fn process(&self, payment: Payment) -> Result<(), PaymentProcessorError> {
        let data = PaymentRequest::from(payment);
        let json_bytes =
            serde_json::to_vec(&data).map_err(|_| PaymentProcessorError::InvalidPayment)?;

        let body = Full::new(Bytes::from(json_bytes));

        let req = Request::builder()
            .method(Method::POST)
            .uri(&self.url)
            .header("content-type", "application/json")
            .body(body)
            .map_err(|_| PaymentProcessorError::InvalidPayment)?;

        let response = self
            .client
            .request(req)
            .await
            .map_err(|_| PaymentProcessorError::Unavailable)?;
        let status = response.status();

        if status == StatusCode::UNPROCESSABLE_ENTITY {
            return Err(PaymentProcessorError::InvalidPayment);
        }

        if (status >= StatusCode::INTERNAL_SERVER_ERROR
            || status == StatusCode::TOO_MANY_REQUESTS
            || status == StatusCode::REQUEST_TIMEOUT)
        {
            return Err(PaymentProcessorError::Unavailable);
        }

        Ok(())
    }
}
