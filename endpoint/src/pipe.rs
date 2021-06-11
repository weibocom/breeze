use discovery::ServiceDiscover;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use std::io::Result;

pub struct Pipe<D> {
    stream: TcpStream,
    _mark: std::marker::PhantomData<D>,
}

impl<D> Pipe<D> {
    #[inline]
    pub async fn from_discovery(discovery: D) -> Result<Self>
    where
        D: ServiceDiscover<Item = String>,
    {
        let addr = discovery.get();
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream: stream,
            _mark: Default::default(),
        })
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::ReadBuf;
impl<D> AsyncRead for Pipe<D>
where
    D: Unpin,
{
    #[inline]
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl<D> AsyncWrite for Pipe<D>
where
    D: Unpin,
{
    #[inline]
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }
    #[inline]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }
    #[inline]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}
