pub mod listener;

mod stream;
pub use stream::Stream;

pub enum SocketAddr {
    Tcp(std::net::SocketAddr),
    Unix(tokio::net::unix::SocketAddr),
}
