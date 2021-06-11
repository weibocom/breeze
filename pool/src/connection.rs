use super::Check;
use net::Stream;

pub struct Connection {
    broken: bool,
    stream: Stream,
}

impl From<Stream> for Connection {
    fn from(stream: Stream) -> Self {
        Self {
            stream,
            broken: false,
        }
    }
}

// 控制connection创建时是否需要检查状态，回收时是否需要主动关闭。
impl Check for Connection {
    #[inline]
    fn has_broken(&self) -> bool {
        self.broken
    }
}

use std::ops::Deref;
impl Deref for Connection {
    type Target = Stream;
    fn deref(&self) -> &Self::Target {
        &self.stream
    }
}

use std::ops::DerefMut;
impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.stream
    }
}
use std::io::ErrorKind;

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::io::Error;
impl Connection {
    fn check_err(&mut self, e: Error) -> Error {
        match e.kind() {
            ErrorKind::BrokenPipe => {
                self.broken = true;
            }
            _ => {}
        };
        e
    }
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<Result<()>> {
        // 处理error的类型，在这可以控制connection是否关闭
        Pin::new(&mut **self)
            .poll_read(cx, buf)
            .map_err(|e| self.check_err(e))
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        Pin::new(&mut **self)
            .poll_write(cx, buf)
            .map_err(|e| self.check_err(e))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut **self)
            .poll_flush(cx)
            .map_err(|e| self.check_err(e))
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        Pin::new(&mut **self).poll_shutdown(cx).map_err(|e| e)
    }
}
