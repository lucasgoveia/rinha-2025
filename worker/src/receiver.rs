use crate::worker_pool::WorkerPool;
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Semaphore;

pub struct Receiver {
    socket_path: String,
    workers: Arc<WorkerPool>,
    conn_sem: Arc<Semaphore>
}

#[derive(Debug)]
pub enum ReceiverError {
    SocketError(std::io::Error),
}

impl std::fmt::Display for ReceiverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReceiverError::SocketError(e) => write!(f, "Socket error: {}", e),
        }
    }
}

impl std::error::Error for ReceiverError {}

impl Receiver {
    pub fn new(socket_path: String, workers: Arc<WorkerPool>) -> Self {
        Self {
            socket_path,
            workers,
            conn_sem: Arc::new(Semaphore::new(512))
        }
    }

    pub async fn start(&mut self) -> Result<(), ReceiverError> {
        tracing::info!("Starting receiver");
        if std::fs::metadata(&self.socket_path).is_ok() {
            let _ = std::fs::remove_file(&self.socket_path);
        }

        let listener = UnixListener::bind(&self.socket_path).map_err(ReceiverError::SocketError)?;

        if let Err(e) = std::fs::set_permissions(
            &self.socket_path,
            std::os::unix::fs::PermissionsExt::from_mode(0o600),
        ) {
            tracing::warn!(error = %e, "Failed to set permissions on socket");
        }

        self.accept_loop(listener).await;

        Ok(())
    }

    async fn accept_loop(&self, listener: UnixListener) {
        let workers = Arc::clone(&self.workers);

        tracing::info!("Listening on {}", self.socket_path);

        loop {
            tracing::debug!("Waiting for connection");
            match listener.accept().await {
                Ok((stream, addr)) => {
                    tracing::info!(?addr, "Accepted UDS connection");

                    let workers_clone = Arc::clone(&workers);
                    let semaphore = Arc::clone(&self.conn_sem);

                    tokio::task::spawn(async move {
                        let _permit = semaphore.acquire().await.unwrap();
                        Self::read_producer(stream, workers_clone).await;
                    });
                }
                Err(e) => {
                    tracing::error!(error = %e, "Failed to accept connection");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        }
    }

    async fn read_producer(stream: UnixStream, workers: Arc<WorkerPool>) {
        let mut reader = BufReader::with_capacity(8192, stream);
        let mut buffer = Vec::with_capacity(1024);

        loop {
            match reader.read_until(b'\n', &mut buffer).await {
                Ok(0) => {
                    tracing::info!("Read producer disconnected");
                    return;
                }
                Ok(_) => {
                    if buffer.last() == Some(&b'\n') {
                        buffer.pop();
                    }

                    if !buffer.is_empty() {
                        let bytes = Bytes::copy_from_slice(&buffer);
                        if let Err(e) = workers.submit(bytes).await {
                            tracing::warn!(error = %e, "Failed to submit message to worker pool");
                        }
                    }

                    buffer.clear();
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Error reading from connection");
                    return;
                }
            }
        }
    }
}
