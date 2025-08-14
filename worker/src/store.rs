use crate::payment::Payment;
use futures_util::pin_mut;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::{ToSql, Type};

#[derive(Debug)]
pub enum StoreError {
    PushPaymentError,
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::PushPaymentError => write!(f, "push payment into the store failed"),
        }
    }
}

impl std::error::Error for StoreError {}

pub struct Store {
    dbpool: Arc<deadpool_postgres::Pool>,
    sender: Option<mpsc::Sender<Payment>>,
}

impl Store {
    pub fn new(dbpool: deadpool_postgres::Pool) -> Self {
        Self {
            dbpool: Arc::new(dbpool),
            sender: None,
        }
    }

    pub async fn init(&mut self) {
        let (sender, receiver) = mpsc::channel(16 * 1024);

        self.sender = Some(sender);
        let dbpool_clone = self.dbpool.clone();
        tokio::spawn(async move {
            Self::insert_loop(receiver, dbpool_clone).await;
        });
    }

    async fn insert_loop(
        mut receiver: mpsc::Receiver<Payment>,
        dbpool: Arc<deadpool_postgres::Pool>,
    ) {
        let mut buffer = Vec::<Payment>::with_capacity(256);

        loop {
            loop {
                match receiver.try_recv() {
                    Ok(item) => buffer.push(item),
                    Err(TryRecvError::Empty) => break, // No more items now
                    Err(TryRecvError::Disconnected) => {
                        // Channel closed, maybe flush and exit loop
                        if !buffer.is_empty() {
                            _ = Self::batch_payments(&dbpool, &buffer).await;
                        }
                        return;
                    }
                }
            }

            if buffer.len() == 1 {
                let payment = buffer.pop().unwrap();
                _ = Self::insert_payment(&dbpool, &payment).await;
            } else if buffer.len() > 1 {
                let payments = std::mem::take(&mut buffer);
                _ = Self::batch_payments(&dbpool, &payments).await;
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    pub async fn push_payment(&self, payment: Payment) -> Result<(), StoreError> {
        match &self.sender {
            Some(sender) => {
                sender
                    .try_send(payment)
                    .map_err(|_| StoreError::PushPaymentError)?;
                Ok(())
            }
            None => Err(StoreError::PushPaymentError),
        }
    }

    async fn batch_payments(dbpool: &Arc<deadpool_postgres::Pool>, payments: &[Payment]) {
        if let Ok(client) = dbpool.get().await {
            if let Ok(sink) = client
                .copy_in("COPY payments (amount, requested_at, service_used, correlation_id) FROM STDIN BINARY")
                .await
            {
                let writer = BinaryCopyInWriter::new(
                    sink,
                    &[Type::NUMERIC, Type::TIMESTAMPTZ, Type::ANYENUM, Type::UUID],
                );
                pin_mut!(writer);

                for payment in payments {
                    if let Err(e) = writer.as_mut()
                        .write(&[
                            &payment.amount,
                            &payment.requested_at,
                            &payment.processor,
                            &payment.correlation_id,
                        ])
                        .await {
                        tracing::error!("failed to write payments batch: {}", e);
                    }
                }

                if let Err(e) = writer.finish().await {
                    tracing::error!("failed to finish payments batch: {}", e);
                }
            }
        } else {
            tracing::error!("failed to get a client from the pool");
        }
    }

    async fn insert_payment(
        dbpool: &Arc<deadpool_postgres::Pool>,
        payment: &Payment,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let conn = dbpool.get().await?;

        let stmt = conn.prepare(
            "INSERT INTO payments (amount, requested_at, service_used, correlation_id) VALUES ($1, $2, $3, $4)"
        )
            .await?;

        conn.execute(
            &stmt,
            &[
                &payment.amount,
                &payment.requested_at,
                &payment.processor,
                &payment.correlation_id,
            ],
        )
        .await?;

        Ok(())
    }
}
