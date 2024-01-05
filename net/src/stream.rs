//use tokio::io::BufStream;
macro_rules! define_stream {
    ($($name:expr, $var:ident, $t:ty, $listener:ty, $addr:ty);+) => {
#[derive(Debug)]
pub enum Stream {
    $(
        $var($t),
    )+
}
impl tokio::io::AsyncRead for Stream {
    #[inline]
    fn poll_read( self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            $(
            Self::$var(ref mut stream) => tokio::io::AsyncRead::poll_read(Pin::new(stream), cx, buf),
            )+
        }
    }
}

impl tokio::io::AsyncWrite for Stream {
    #[inline]
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        match self.get_mut() {
            $(
            Stream::$var(ref mut stream) => tokio::io::AsyncWrite::poll_write(Pin::new(stream), cx, buf),
            )+
        }
    }
    #[inline]
    fn poll_flush(self: Pin<&mut Self>,cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            $(
            Stream::$var(ref mut stream) => tokio::io::AsyncWrite::poll_flush(Pin::new(stream), cx),
            )+
        }
    }
    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>,cx: &mut Context<'_>) -> Poll<Result<()>> {
        match self.get_mut() {
            $(
            Stream::$var(ref mut stream) => tokio::io::AsyncWrite::poll_shutdown(Pin::new(stream), cx),
            )+
        }
    }
}


pub enum SocketAddr {
    $(
    $var($addr),
    )+
}

// listener

pub enum Listener {
    $(
    $var($listener),
    )+
}
impl Listener {
    pub async fn bind(protocol: &str, addr: &str) -> std::io::Result<Self> {
        match protocol.to_lowercase().as_str() {
            $(
            $name  => Ok(Self::$var(<$listener>::binding(addr).await?)),
            )+
            _ => Err(Error::new(ErrorKind::InvalidInput, addr)),
        }
    }

    pub async fn accept(&self) -> std::io::Result<(Stream, SocketAddr)> {
        match self {
            $(
            Self::$var(l) => {
                let (mut stream, addr) = l.accept().await?;
                use crate::StreamInit;
                stream.init();
                Ok((Stream::$var(stream), SocketAddr::$var(addr)))
            }
            )+
        }
    }
}


    };
} // end of macro define_stream

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

trait Bind: Sized {
    async fn binding(addr: &str) -> Result<Self>;
}
impl Bind for tokio::net::TcpListener {
    async fn binding(addr: &str) -> Result<Self> {
        Self::bind(addr).await
    }
}
impl Bind for tokio::net::UnixListener {
    async fn binding(addr: &str) -> Result<Self> {
        Self::bind(addr)
    }
}

define_stream!(
    "unix", Unix,    tokio::net::UnixStream,     tokio::net::UnixListener,    tokio::net::unix::SocketAddr;
    "tcp",  Tcp,     tokio::net::TcpStream,      tokio::net::TcpListener,     std::net::SocketAddr
);
