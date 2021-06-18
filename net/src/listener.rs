use super::SocketAddr;
use super::Stream;

use std::io::Error;
use std::io::ErrorKind;

use tokio::net::{TcpListener, UnixListener};

pub enum Listener {
    Unix(UnixListener),
    Tcp(TcpListener),
}

impl Listener {
    pub async fn bind(protocol: &str, addr: &str) -> std::io::Result<Self> {
        match protocol {
            "unix" => Ok(Listener::Unix(UnixListener::bind(addr)?)),
            "tcp" => Ok(Listener::Tcp(TcpListener::bind(addr).await?)),
            _ => Err(Error::new(ErrorKind::InvalidInput, addr)),
        }
    }

    pub async fn accept(&self) -> std::io::Result<(Stream, SocketAddr)> {
        match self {
            Listener::Unix(l) => {
                let (stream, addr) = l.accept().await?;
                Ok((stream.into(), SocketAddr::Unix(addr)))
            }
            Listener::Tcp(l) => {
                let (stream, addr) = l.accept().await?;
                //stream.set_nodelay(true)?;
                Ok((stream.into(), SocketAddr::Tcp(addr)))
            }
        }
    }
}
