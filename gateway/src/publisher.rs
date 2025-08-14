use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::net::UnixStream;
use tokio::sync::{Mutex, mpsc};

#[derive(Debug)]
pub enum PublisherError {
    ConnectionFailed(std::io::Error),
    WriteError(std::io::Error),
    Timeout,
}

impl std::fmt::Display for PublisherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PublisherError::ConnectionFailed(e) => write!(f, "Connection failed: {}", e),
            PublisherError::WriteError(e) => write!(f, "Write error: {}", e),
            PublisherError::Timeout => write!(f, "Operation timed out")
        }
    }
}

impl std::error::Error for PublisherError {}

pub struct Publisher {
    socket_path: String,
    max_conns: usize,
    conn_pool: mpsc::Sender<UnixStream>,
    conn_receiver: Arc<Mutex<mpsc::Receiver<UnixStream>>>,
    connect_timeout: Duration,
    pool_size: Arc<AtomicUsize>,
}

impl Publisher {
    pub async fn new(socket_path: String, max_conns: usize) -> Result<Self, PublisherError> {
        let (sender, receiver) = mpsc::channel(max_conns);

        // Pre-populate the pool with connections
        let mut initial_connections = 0;
        for _ in 0..std::cmp::min(max_conns, 5) {
            if let Ok(conn) = tokio::time::timeout(
                Duration::from_millis(100),
                UnixStream::connect(&socket_path),
            ).await {
                if let Ok(conn) = conn {
                    if sender.send(conn).await.is_ok() {
                        initial_connections += 1;
                    }
                }
            }
        }

        Ok(Publisher {
            socket_path,
            max_conns,
            conn_pool: sender,
            conn_receiver: Arc::new(Mutex::new(receiver)),
            connect_timeout: Duration::from_millis(50), // Reduced timeout
            pool_size: Arc::new(AtomicUsize::new(initial_connections)),
        })

    }

    pub async fn publish(&self, msg: &[u8]) -> Result<(), PublisherError> {
        let mut conn = self.acquire().await?;

        let mut writer = BufWriter::with_capacity(1024, &mut conn);

        let write_result = async {
            writer.write_all(msg).await?;
            writer.write_all(b"\n").await?;
            writer.flush().await?;
            Ok::<(), std::io::Error>(())
        }.await;

        match write_result {
            Ok(_) => {
                self.release(conn).await;
                Ok(())
            },
            Err(e ) => {
                let _ = conn.shutdown().await;
                self.pool_size.fetch_sub(1, Ordering::Relaxed);
                tokio::task::spawn({
                    let publisher = self.clone();
                    async move {
                        publisher.replace().await;
                    }
                });
                Err(PublisherError::WriteError(e))
            }
        }
    }

    async fn acquire(&self) -> Result<UnixStream, PublisherError> {
        if let Ok(mut receiver) = self.conn_receiver.try_lock() {
            if let Ok(conn) = receiver.try_recv() {
                self.pool_size.fetch_sub(1, Ordering::Relaxed);
                return Ok(conn);
            }
        }

        // Create new connection if pool is empty
        tokio::time::timeout(self.connect_timeout, UnixStream::connect(&self.socket_path))
            .await
            .map_err(|_| PublisherError::Timeout)?
            .map_err(PublisherError::ConnectionFailed)
    }

    async fn release(&self, conn: UnixStream) {
        if self.pool_size.load(Ordering::Relaxed) < self.max_conns {
            if self.conn_pool.try_send(conn).is_ok() {
                self.pool_size.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn replace(&self) {
        if let Ok(conn) = tokio::time::timeout(
            self.connect_timeout,
            UnixStream::connect(&self.socket_path)
        ).await {
            if let Ok(conn) = conn {
                if self.conn_pool.try_send(conn).is_ok() {
                    self.pool_size.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

impl Clone for Publisher {
    fn clone(&self) -> Self {
        Self {
            socket_path: self.socket_path.clone(),
            max_conns: self.max_conns,
            conn_pool: self.conn_pool.clone(),
            conn_receiver: self.conn_receiver.clone(),
            connect_timeout: self.connect_timeout,
            pool_size: self.pool_size.clone(),
        }
    }
}

unsafe impl Send for Publisher {}
unsafe impl Sync for Publisher {}

