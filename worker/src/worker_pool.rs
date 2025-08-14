use crate::health_monitor::HealthMonitor;
use crate::payment::Payment;
use crate::payment_message::PaymentMessage;
use crate::payment_processor::{PaymentProcessor, PaymentProcessorError};
use crate::processor_type::ProcessorType;
use crate::store::Store;
use bytes::Bytes;
use std::collections::BinaryHeap;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use time::{UtcDateTime, UtcOffset};
use tokio::sync::mpsc;

use tokio::time::Instant;

#[derive(Debug)]
pub enum WorkerPoolError {
    JsonParseError(serde_json::Error),
    QueueClosed,
    QueueFull,
    PaymentFailed(PaymentProcessorError),
    ProcessorsUnavailable,
}
impl std::fmt::Display for WorkerPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkerPoolError::JsonParseError(e) => write!(f, "JSON parse error: {}", e),
            WorkerPoolError::QueueClosed => write!(f, "Queue closed"),
            WorkerPoolError::QueueFull => write!(f, "Queue full"),
            WorkerPoolError::PaymentFailed(e) => write!(f, "Payment failed: {}", e),
            WorkerPoolError::ProcessorsUnavailable => write!(f, "No processors available"),
        }
    }
}

impl std::error::Error for WorkerPoolError {}

const BUFFER_SIZE: usize = 32768;
const MAX_RETRIES: u32 = 50;
const BASE_BACKOFF_MS: u64 = 500;
const MAX_BACKOFF_MS: u64 = 2_000;
const JITTER_FRACTION: f64 = 0.2;

struct RetryItem {
    msg: PaymentMessage,
    next_attempt: Instant, 
}

impl PartialEq for RetryItem {
    fn eq(&self, other: &Self) -> bool {
        self.next_attempt == other.next_attempt
    }
}

impl Eq for RetryItem {}

impl PartialOrd for RetryItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RetryItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.next_attempt.cmp(&self.next_attempt)
    }
}

#[derive(Clone)]
pub struct WorkerDependencies {
    health_monitor: Arc<HealthMonitor>,
    default_processor: Arc<PaymentProcessor>,
    fallback_processor: Arc<PaymentProcessor>,
    store: Arc<Store>,
}

#[derive(Clone)]
pub struct WorkerPool {
    senders: Vec<mpsc::Sender<PaymentMessage>>,
    num_workers: usize,
    deps: WorkerDependencies,
    next_worker: Arc<AtomicUsize>,
}

impl WorkerPool {
    pub fn new(
        num_workers: usize,
        health_monitor: Arc<HealthMonitor>,
        default_processor: Arc<PaymentProcessor>,
        fallback_processor: Arc<PaymentProcessor>,
        store: Arc<Store>,
    ) -> Self {
        Self {
            senders: Vec::with_capacity(num_workers),
            num_workers,
            next_worker: Arc::new(AtomicUsize::new(0)),
            deps: WorkerDependencies {
                health_monitor,
                default_processor,
                fallback_processor,
                store,
            },
        }
    }

    pub async fn submit(&self, msg: Bytes) -> Result<(), WorkerPoolError> {
        if let Ok(msg) = serde_json::from_slice::<PaymentMessage>(&msg) {
            return self.submit_internal(msg).await;
        }
        Ok(())
    }

    async fn submit_internal(&self, msg: PaymentMessage) -> Result<(), WorkerPoolError> {
        if self.senders.is_empty() {
            return Err(WorkerPoolError::QueueClosed);
        }

        thread_local! {
            static COUNTER: std::cell::Cell<usize> = std::cell::Cell::new(0);
        }

        let worker_index = COUNTER.with(|c| {
            let current = c.get();
            let next = (current + 1) % self.senders.len();
            c.set(next);
            current
        });


        self.senders[worker_index]
            .try_send(msg)
            .map_err(|_| WorkerPoolError::QueueClosed)?;

        tracing::debug!("Submitted message to worker {}", worker_index);
        Ok(())
    }

    pub async fn start(&mut self) {
        let mut handles = Vec::new();
        let mut senders = Vec::new();
        let worker_channel_size = BUFFER_SIZE / self.num_workers;
        let (retry_sender, retry_receiver) = mpsc::channel(BUFFER_SIZE);

        for worker_id in 0..self.num_workers {
            let (sender, receiver) = mpsc::channel(worker_channel_size);
            let deps = self.deps.clone();
            let retry_sender_clone = retry_sender.clone();

            let handle = tokio::spawn(async move {
                Self::worker_loop(worker_id, receiver, retry_sender_clone, deps).await;
            });

            handles.push(handle);
            senders.push(sender);
        }

        self.senders = senders;

        let self_clone = self.clone();
        tokio::spawn(async move {
            Self::retry_loop(self_clone, retry_receiver).await;
        });

        tracing::info!("Started {} workers", self.num_workers);
    }

    async fn retry_loop(self, mut retry_receiver: mpsc::Receiver<RetryItem>) {
        let mut heap: BinaryHeap<RetryItem> = BinaryHeap::with_capacity(8 * 1024);

        loop {
            let now = Instant::now();
            while let Some(item) = heap.peek() {
                if item.next_attempt <= now {
                    let item = heap.pop().unwrap();
                    if let Err(e) = self.submit_internal(item.msg).await {
                        tracing::error!("Failed to resubmit retry message: {}", e);
                    }
                } else {
                    break;
                }
            }

            let next_timer = heap
                .peek()
                .map(|item| tokio::time::sleep_until(item.next_attempt));

            tokio::select! {
                item = retry_receiver.recv() => {
                    if let Some(item) = item {
                        heap.push(item);
                    }
                }
                _ = async {
                    match next_timer {
                        Some(timer) => timer.await,
                        None => std::future::pending().await,
                    }
                } => {}
            }
        }
    }

    async fn retry(mut msg: PaymentMessage, retry_sender: &mpsc::Sender<RetryItem>) {
        if msg.retry_count >= MAX_RETRIES {
            tracing::warn!(
                "Max retries exceeded, dropping message: {}",
                msg.correlation_id
            );
            return;
        }

        msg.retry_count += 1;
        let delay = Self::calc_backoff(msg.retry_count);
        let item = RetryItem {
            msg,
            next_attempt: Instant::now() + std::time::Duration::from_millis(delay),
        };

        if let Err(_) = retry_sender.try_send(item) {
            tracing::warn!("Retry queue is full, dropping message");
        }
    }

    fn calc_backoff(retry_count: u32) -> u64 {
        let delay = BASE_BACKOFF_MS * (1_u64 << retry_count.min(10)); // Cap the exponential growth
        let delay = delay.min(MAX_BACKOFF_MS);

        let jitter_range = (delay as f64 * JITTER_FRACTION) as u64;
        let pseudo = retry_count.wrapping_mul(1103515245).wrapping_add(12345) as u64;
        let jitter = pseudo % (2 * jitter_range).max(1);

        delay.saturating_sub(jitter_range).saturating_add(jitter)
    }

    async fn worker_loop(
        id: usize,
        mut receiver: mpsc::Receiver<PaymentMessage>,
        retry_sender: mpsc::Sender<RetryItem>,
        deps: WorkerDependencies,
    ) {
        while let Some(msg) = receiver.recv().await {
            if let Err(e) = Self::process_message(id, &msg, &deps).await {
                tracing::info!(worker_id = id, error = %e, "Worker failed to process message retrying");
                Self::retry(msg, &retry_sender).await
            }
        }
        tracing::info!(worker_id = id, "Worker shutting down - channel closed");
    }

    async fn process_message(
        _id: usize,
        msg: &PaymentMessage,
        deps: &WorkerDependencies,
    ) -> Result<(), WorkerPoolError> {
        match deps.health_monitor.next_processor().await {
            Ok(processor_type) => match processor_type {
                ProcessorType::Default  => Self::process_default(msg, deps).await,
                ProcessorType::Fallback => Self::process_fallback(msg, deps).await,
            },
            Err(_) => Err(WorkerPoolError::ProcessorsUnavailable),
        }
    }

    async fn process_default(
        msg: &PaymentMessage,
        deps: &WorkerDependencies,
    ) -> Result<(), WorkerPoolError> {
        let payment = Payment::new(
            msg.amount,
            msg.correlation_id,
            ProcessorType::Default,
            UtcDateTime::now().to_offset(UtcOffset::UTC),
        );

        match deps.default_processor.process(payment.clone()).await {
            Ok(_) => {
                if let Err(e) = deps.store.push_payment(payment).await {
                    tracing::error!("Failed to insert payment into database: {}", e);
                }
                Ok(())
            }
            Err(e) => {
                tracing::info!("Payment failed to process");
                Err(WorkerPoolError::PaymentFailed(e))
            }
        }
    }

    async fn process_fallback(
        msg: &PaymentMessage,
        deps: &WorkerDependencies,
    ) -> Result<(), WorkerPoolError> {
        let payment = Payment::new(
            msg.amount,
            msg.correlation_id,
            ProcessorType::Fallback,
            UtcDateTime::now().to_offset(UtcOffset::UTC),
        );

        match deps.fallback_processor.process(payment.clone()).await {
            Ok(_) => {
                if let Err(e) = deps.store.push_payment(payment).await {
                    tracing::error!("Failed to insert payment into database: {}", e);
                }
                Ok(())
            }
            Err(e) => {
                tracing::info!("Payment failed to process");
                Err(WorkerPoolError::PaymentFailed(e))
            }
        }
    }
}
