mod uds_round_robin;

use crate::uds_round_robin::UdsRoundRobin;
use async_trait::async_trait;
use pingora::prelude::*;
use std::sync::Arc;

pub struct LB(Arc<UdsRoundRobin>);

#[async_trait]
impl ProxyHttp for LB {
    type CTX = ();
    fn new_ctx(&self) -> () {
        ()
    }

    async fn upstream_peer(
        &self,
        _session: &mut Session,
        _ctx: &mut Self::CTX,
    ) -> Result<Box<HttpPeer>> {
        let path = self
            .0
            .select()
            .ok_or_else(|| Error::new(ErrorType::InternalError))?;

        let peer = Box::new(HttpPeer::new_uds(path.as_str(), false, String::new())?);

        Ok(peer)
    }
}

fn main() {
    let mut my_server = Server::new(None).unwrap();
    my_server.bootstrap();

    let uds_balancer = UdsRoundRobin::new(vec![
        "/tmp/server1.sock".to_string(),
        "/tmp/server2.sock".to_string(),
    ]);

    let mut lb = http_proxy_service(&my_server.configuration, LB(Arc::new(uds_balancer)));
    lb.add_tcp("0.0.0.0:9999");

    my_server.add_service(lb);

    my_server.run_forever();
}
