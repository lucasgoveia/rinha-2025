mod load_balancer;

use std::net::SocketAddr;
use std::sync::Arc;

use crate::load_balancer::{UnixLoadBalancer, UnixLoadBalancerConfig};
use http_body_util::combinators::BoxBody;
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpSocket;

use tracing::Level;
use tracing_subscriber::FmtSubscriber;

use hyper::body::{Bytes};

enum ProxyResponse {
    Success(Response<Incoming>),
    Error,
}

impl From<ProxyResponse> for Response<BoxBody<Bytes, hyper::Error>> {
    fn from(resp: ProxyResponse) -> Self {
        match resp {
            ProxyResponse::Success(r) => r.map(|body| BoxBody::new(body)),
            ProxyResponse::Error => Response::builder()
                .status(502)
                .body(BoxBody::new(
                    http_body_util::Empty::new().map_err(|never| match never {}),
                ))
                .unwrap(),
        }
    }
}

async fn proxy_service(
    balancer: Arc<UnixLoadBalancer>,
    req: Request<Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    let method = req.method().clone();
    let uri = req.uri().clone();

    let response = match balancer.forward_request(method, uri, req.into_body()).await {
        Ok(resp) => ProxyResponse::Success(resp),
        Err(_) => ProxyResponse::Error,
    };

    Ok(response.into())
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::WARN)
        .with_ansi(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let balancer_config = UnixLoadBalancerConfig::from_env();
    let lb = Arc::new(UnixLoadBalancer::new(balancer_config));

    let addr = SocketAddr::from(([0, 0, 0, 0], 9999));

    let socket = TcpSocket::new_v4().unwrap();
    socket.set_reuseaddr(true).unwrap();
    socket.set_reuseport(true).unwrap();
    socket.set_recv_buffer_size(16 * 1024).unwrap();
    socket.set_send_buffer_size(16 * 1024).unwrap();

    socket.bind(addr).unwrap();
    let listener = socket.listen(16 * 1024).unwrap();

    loop {
        let (tcp_stream, _) = listener.accept().await.unwrap();

        tcp_stream.set_nodelay(true).unwrap();
        tcp_stream.set_ttl(64).unwrap();

        let lb_clone = lb.clone();

        tokio::spawn(async move {
            let io = TokioIo::new(tcp_stream);

            let service = service_fn(move |req| {
                let balancer = lb_clone.clone();
                proxy_service(balancer, req)
            });

            let conn = http1::Builder::new()
                .keep_alive(true)
                .half_close(false)
                .writev(true)
                .max_buf_size(16 * 1024)
                .preserve_header_case(false)
                .title_case_headers(false)
                .serve_connection(io, service);

            if let Err(err) = conn.await {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
