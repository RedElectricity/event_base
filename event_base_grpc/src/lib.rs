use crate::server::EventBaseService;
use crate::server::event_base::event_base_server::EventBaseServer;
use std::net::SocketAddr;
use tonic::transport::Server;

pub mod server;

pub async fn serve(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let service = EventBaseService;
    Server::builder()
        .add_service(EventBaseServer::new(service))
        .serve(addr)
        .await?;
    Ok(())
}
