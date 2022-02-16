mod stream;
pub use stream::*;

pub trait StreamInit {
    #[inline]
    fn init(&mut self) {}
}

impl StreamInit for tokio::net::UnixStream {}

impl StreamInit for tokio::net::TcpStream {
    #[inline]
    fn init(&mut self) {
        let _ = self.set_nodelay(true);
    }
}
