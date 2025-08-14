use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct PaymentMessage {
    pub amount: Decimal,
    #[serde(rename = "correlationId")]
    pub correlation_id: uuid::Uuid,
    #[serde(default)]
    pub retry_count: u32,
}

impl PaymentMessage {
    pub fn new(amount: Decimal, correlation_id: uuid::Uuid) -> Self {
        Self {
            amount,
            correlation_id,
            retry_count: 0,
        }
    }
}
