use super::{AsyncPipeToPingPongChanWrite, AsyncWriteAll};
use crate::parser::Protocol;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, BytesMut};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use futures::ready;

pub struct PipeToPingPongChanWrite<P, S> {
    shutdown: bool,
    w_buf: BytesMut,
    parser: P,
    inner: S,
}

impl<P, S> PipeToPingPongChanWrite<P, S> {
    pub fn from_stream(parser: P, stream: S) -> Self {
        Self {
            shutdown: false,
            w_buf: BytesMut::with_capacity(2048),
            parser: parser,
            inner: stream,
        }
    }
}

impl<P, S> AsyncPipeToPingPongChanWrite for PipeToPingPongChanWrite<P, S>
where
    P: Protocol,
    S: AsyncWriteAll + AsyncWrite + Unpin + AsyncRead,
{
}
impl<P, S> AsyncWriteAll for PipeToPingPongChanWrite<P, S> {}

impl<P, S> PipeToPingPongChanWrite<P, S> {}

impl<P, S> AsyncWrite for PipeToPingPongChanWrite<P, S>
where
    P: Protocol,
    S: AsyncWriteAll + AsyncWrite + Unpin,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        println!("bytes received {} {:?}", buf.len(), buf);
        let me = &mut *self;
        if me.shutdown {
            return Poll::Ready(Err(Error::new(
                ErrorKind::BrokenPipe,
                "write to a closed chan",
            )));
        }
        let mut offset = 0;
        while offset < buf.len() {
            let (_, buf) = buf.split_at(offset);
            // 确保字节数满足协议要求
            if me.w_buf.len() + buf.len() < me.parser.min_size() {
                me.w_buf.put_slice(buf);
                offset += buf.len();
                continue;
            }
            let first = me.w_buf.len() == 0;
            let mut start = 0;
            let req = if first {
                buf
            } else {
                start = me.w_buf.len();
                me.w_buf.put_slice(buf);
                &me.w_buf
            };
            let (done, n) = me.parser.probe_request_eof(req);
            offset += n - start;
            debug_assert!(n > 0);
            if !done {
                if first {
                    me.w_buf.put_slice(&buf[..n]);
                }
                continue;
            }
            let cmd = if me.w_buf.len() > 0 {
                &me.w_buf
            } else {
                // zero copy。
                &buf[..n]
            };
            // 解析出一个request，写入到chan的下游
            let w = ready!(Pin::new(&mut me.inner).poll_write(cx, cmd))?;
            assert_eq!(w, cmd.len());
            me.w_buf.clear();
        }
        Poll::Ready(Ok(offset))
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        let mut w = Pin::new(&mut me.inner);
        if me.w_buf.len() > 0 {
            let w = ready!(w.as_mut().poll_write(cx, &me.w_buf))?;
            debug_assert_eq!(w, me.w_buf.len());
            me.w_buf.clear();
        }
        w.as_mut().poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        me.shutdown = true;
        let mut w = Pin::new(&mut me.inner);
        ready!(w.as_mut().poll_flush(cx))?;
        w.as_mut().poll_shutdown(cx)
    }
}
impl<P, S> AsyncRead for PipeToPingPongChanWrite<P, S>
where
    P: Protocol,
    S: AsyncRead + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        if self.shutdown {
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
