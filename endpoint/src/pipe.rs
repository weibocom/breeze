use discovery::ServiceDiscover;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use std::io::Result;

pub struct Pipe<P> {
    stream: TcpStream,
    _mark: std::marker::PhantomData<P>,
}

impl<P> Pipe<P> {
    #[inline]
    pub async fn from_discovery<D>(_p: P, discovery: D) -> Result<Self>
    where
        D: ServiceDiscover<Topology<P>>,
    {
        let addr = discovery.do_with(|_t| "127.0.0.1:11211".to_owned());
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
impl<P> AsyncRead for Pipe<P>
where
    P: Unpin,
{
    #[inline]
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl<P> AsyncWrite for Pipe<P>
where
    P: Unpin,
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

use super::Topology;
#[derive(Clone, Default)]
pub struct PipeTopology<P> {
    _mark: std::marker::PhantomData<P>,
}

impl<P> discovery::Topology for PipeTopology<P>
where
    P: Clone,
{
    fn update(&mut self, _cfg: &str, _name: &str) {
        todo!()
    }
}
impl<P> left_right::Absorb<(String, String)> for PipeTopology<P>
where
    P: Clone,
{
    fn absorb_first(&mut self, cfg: &mut (String, String), _other: &Self) {
        discovery::Topology::update(self, &cfg.0, &cfg.1);
    }
    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }
}

impl<P> From<P> for PipeTopology<P> {
    fn from(_parser: P) -> Self {
        Self {
            _mark: Default::default(),
        }
    }
}
