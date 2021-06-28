/// 按一次获取多个key的请求时，类似memcache与redis的gets时，一种
/// 方式是把keys解析出来，然后分发分发给不同的shards；另外一种方
/// 式是把所有的keys发送给所有的后端，然后合并。这种方式没有keys
/// 的解析，会更加高效。适合到shards的分片不多的场景，尤其是一般
/// 一个keys的请求req通常只包含key，所以额外的load会比较低。
/// 发送给所有sharding的请求，有一个成功，即认定为成功。

pub struct AsyncMultiGet<S, P> {
    // 当前从哪个shard开始发送请求
    idx: usize,
    // 成功发送请求的shards
    writes: Vec<bool>,
    shards: Vec<S>,
    parser: P,
    response: Option<ResponseItem>,
    eof: usize,
}

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use super::{AsyncReadAll, AsyncWriteAll, ResponseItem};
use tokio::io::{AsyncRead, AsyncWrite};

use futures::ready;

impl<S, P> AsyncWriteAll for AsyncMultiGet<S, P> {}

impl<S, P> AsyncMultiGet<S, P> {
    pub fn from_shard(shards: Vec<S>, p: P) -> Self {
        Self {
            idx: 0,
            writes: vec![false; shards.len()],
            shards: shards,
            parser: p,
            response: None,
            eof: 0,
        }
    }
}

impl<S, P> AsyncWrite for AsyncMultiGet<S, P>
where
    S: AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin,
{
    // 只要有一个shard成功就算成功,如果所有的都写入失败，则返回错误信息。
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        let me = &mut *self;
        let offset = me.idx;
        let mut success = false;
        let mut last_err = None;
        for i in offset..me.shards.len() {
            me.writes[i] = false;
            match ready!(Pin::new(unsafe { me.shards.get_unchecked_mut(i) }).poll_write(cx, buf)) {
                Err(e) => last_err = Some(e),
                _ => success = true,
            }
            me.writes[i] = true;
            me.idx += 1;
        }
        me.idx = 0;
        if success {
            Poll::Ready(Ok(buf.len()))
        } else {
            Poll::Ready(Err(last_err.unwrap_or_else(|| {
                Error::new(ErrorKind::Other, "no request sent.")
            })))
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        // 为了简单。flush时不考虑pending，即使从pending恢复，也可以把之前的poll_flush重新操作一遍。
        // poll_flush是幂等的
        let me = &mut *self;
        for (i, &success) in me.writes.iter().enumerate() {
            if success {
                ready!(Pin::new(unsafe { me.shards.get_unchecked_mut(i) }).poll_flush(cx))?;
            }
        }
        Poll::Ready(Ok(()))
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        let mut last_err = None;
        for (i, &success) in me.writes.iter().enumerate() {
            if success {
                match ready!(Pin::new(unsafe { me.shards.get_unchecked_mut(i) }).poll_shutdown(cx))
                {
                    Err(e) => last_err = Some(e),
                    _ => {}
                }
            }
        }
        match last_err {
            Some(e) => Poll::Ready(Err(e)),
            _ => Poll::Ready(Ok(())),
        }
    }
}

impl<S, P> AsyncReadAll for AsyncMultiGet<S, P>
where
    S: AsyncReadAll + Unpin,
    P: Unpin + crate::Protocol,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<ResponseItem>> {
        let me = &mut *self;
        let mut last_err = None;
        let offset = me.idx;
        for i in offset..me.shards.len() {
            if me.writes[i] {
                let mut r = Pin::new(unsafe { me.shards.get_unchecked_mut(i) });
                match ready!(r.as_mut().poll_next(cx)) {
                    Err(e) => last_err = Some(e),
                    Ok(mut response) => {
                        me.eof = me.parser.trim_eof(&response);
                        response.backwards(me.eof);
                        match me.response.as_mut() {
                            Some(item) => item.append(response),
                            None => me.response = Some(response),
                        }
                    }
                }
            }
            me.idx += 1;
        }
        me.idx = 0;
        let eof = me.eof;
        me.eof = 0;
        me.response
            .take()
            .map(|mut item| {
                item.advance(eof);
                Poll::Ready(Ok(item))
            })
            .unwrap_or_else(|| {
                Poll::Ready(Err(last_err.unwrap_or_else(|| {
                    Error::new(ErrorKind::Other, "all poll read failed")
                })))
            })
    }

    fn poll_done(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        for (i, &success) in me.writes.iter().enumerate() {
            if success {
                ready!(Pin::new(unsafe { me.shards.get_unchecked_mut(i) }).poll_done(cx))?;
            }
        }
        Poll::Ready(Ok(()))
    }
}
