use crate::{AsyncPipeToPingPongChanWrite, AsyncReadAll, AsyncWriteAll, Response};
use protocol::Protocol;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use bytes::{BufMut, BytesMut};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use futures::ready;

pub struct PipeToPingPongChanWrite<P, S> {
    shutdown: bool,
    w_buf: BytesMut,
    parser: P,
    inner: S,
    response: Option<Response>,
    // 已经写完，但没有读取的请求数量
    pending: usize,
    waker: Option<Waker>,
}

impl<P, S> PipeToPingPongChanWrite<P, S> {
    pub fn from_stream(parser: P, stream: S) -> Self {
        Self {
            shutdown: false,
            w_buf: BytesMut::with_capacity(2048),
            parser: parser,
            inner: stream,
            response: None,
            pending: 0,
            waker: None,
        }
    }
}

impl<P, S> AsyncPipeToPingPongChanWrite for PipeToPingPongChanWrite<P, S>
where
    P: Protocol,
    S: AsyncWriteAll + Unpin + AsyncReadAll,
{
}

impl<P, S> PipeToPingPongChanWrite<P, S> {}

impl<P, S> AsyncWrite for PipeToPingPongChanWrite<P, S>
where
    P: Protocol,
    S: AsyncWriteAll + Unpin,
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
        if me.pending > 0 {
            me.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        // 不满足协议最低字节数要求, 先缓存
        if me.w_buf.len() + buf.len() < me.parser.min_size() {
            me.w_buf.put_slice(buf);
            return Poll::Ready(Ok(buf.len()));
        }
        let mut put = false;
        let w_len = me.w_buf.len();
        let req = if me.w_buf.len() == 0 {
            buf
        } else {
            put = true;
            me.w_buf.put_slice(buf);
            &me.w_buf
        };
        let (done, n) = match me.parser.parse_request(req) {
            Ok((done, n)) => (done, n),
            Err(e) => return Poll::Ready(Err(e)),
        };
        debug_assert!(n > 0);
        if !done {
            if !put {
                me.w_buf.put_slice(&buf[..n]);
            }
            return Poll::Ready(Ok(buf.len()));
        }
        let cmd = if me.w_buf.len() > 0 {
            &me.w_buf
        } else {
            // zero copy。
            &buf[..n]
        };
        // 解析出一个request，写入到chan的下游
        ready!(Pin::new(&mut me.inner).poll_write(cx, cmd))?;
        let bytes = cmd.len() - w_len;
        me.w_buf.clear();
        me.pending = 1;

        if let Some(waker) = me.waker.take() {
            waker.wake();
        }
        Poll::Ready(Ok(bytes))
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        me.shutdown = true;
        Poll::Ready(Ok(()))
    }
}
impl<P, S> AsyncRead for PipeToPingPongChanWrite<P, S>
where
    P: Protocol,
    S: AsyncReadAll + Unpin,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buff: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        let mut me = &mut *self;
        if me.shutdown {
            return Poll::Ready(Ok(()));
        }
        if me.pending == 0 {
            me.waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let mut inner = Pin::new(&mut me.inner);
        // 大部分场景不会touch response
        let mut item = match me.response.take() {
            Some(item) => item,
            None => ready!(inner.as_mut().poll_next(cx))?,
        };

        if !item.write_to(buff) {
            me.response = Some(item);
        } else {
            me.pending = 0;
            if let Some(waker) = me.waker.take() {
                waker.wake();
            }
        }
        Poll::Ready(Ok(()))
    }
}
