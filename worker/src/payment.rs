use crate::processor_type::ProcessorType;
use rust_decimal::Decimal;
use time::OffsetDateTime;

#[derive(Debug, Clone)]
pub struct Payment {
    pub amount: Decimal,
    pub correlation_id: uuid::Uuid,
    pub requested_at: OffsetDateTime,
    pub processor: ProcessorType,
}

impl Payment {
    pub fn new(
        amount: Decimal,
        correlation_id: uuid::Uuid,
        processor: ProcessorType,
        now: OffsetDateTime,
    ) -> Self {
        Self {
            amount,
            correlation_id,
            processor,
            requested_at: now,
        }
    }
}
