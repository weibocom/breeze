/// 按一次获取多个key的请求时，类似memcache与redis的gets时，一种
/// 方式是把keys解析出来，然后分发分发给不同的shards；另外一种方
/// 式是把所有的keys发送给所有的后端，然后合并。这种方式没有keys
/// 的解析，会更加高效。适合到shards的分片不多的场景，尤其是一般
/// 一个keys的请求req通常只包含key，所以额外的load会比较低。
/// 发送给所有sharding的请求，有一个成功，即认定为成功。

// TODO 这个文件改为 multi_get_sharding? 待和@icy 确认 fishermen
// 思路： multi_get_sharding 是一种特殊的用于multi get 的shard读取，但支持单层，需要进一步封装为多层访问

pub struct AsyncMultiGetSharding<S, P> {
    // 当前从哪个shard开始发送请求
    idx: usize,
    // 成功发送请求的shards
    writes: Vec<bool>,
    shards: Vec<S>,
    parser: P,
}

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::chan::AsyncWriteAll;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use futures::ready;

impl<S, P> AsyncWriteAll for AsyncMultiGetSharding<S, P> {}

impl<S, P> AsyncMultiGetSharding<S, P> {
    pub fn from_shard(shards: Vec<S>, p: P) -> Self {
        Self {
            idx: 0,
            writes: vec![false; shards.len()],
            shards: shards,
            parser: p,
        }
    }
}

impl<S, P> AsyncWrite for AsyncMultiGetSharding<S, P>
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
        let mut last_err = None;
        for (i, &success) in me.writes.iter().enumerate() {
            if success {
                match ready!(Pin::new(unsafe { me.shards.get_unchecked_mut(i) }).poll_flush(cx)) {
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

impl<S, P> AsyncRead for AsyncMultiGetSharding<S, P>
where
    S: AsyncRead + Unpin,
    P: Unpin + crate::Protocol,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        let me = &mut *self;
        let mut success = false;
        let mut last_err = None;
        let offset = me.idx;
        let mut eof_len = 0;
        for i in offset..me.shards.len() {
            if me.writes[i] {
                let mut r = Pin::new(unsafe { me.shards.get_unchecked_mut(i) });
                let mut c_done = false;
                while !c_done {
                    match r.as_mut().poll_read(cx, buf) {
                        Poll::Pending => {
                            // 如果有数据获取，则返回数据。触发下一次获取。
                            // 如果未获取到数据，则pending
                            return if buf.filled().len() == 0 {
                                Poll::Pending
                            } else {
                                Poll::Ready(Ok(()))
                            };
                        }
                        Poll::Ready(Err(e)) => {
                            c_done = true;
                            last_err = Some(e)
                        }
                        Poll::Ready(Ok(())) => {
                            success = true;
                            // 检查当前请求是否包含请求的结束
                            let filled = buf.filled().len();
                            let (done, n) = me.parser.probe_response_eof(buf.filled());
                            if done {
                                c_done = true;
                                eof_len = filled - n;
                                buf.set_filled(n);
                            }
                        }
                    }
                }
            }
            me.idx += 1;
        }
        me.idx = 0;
        if success {
            // 把最后一次成功请求的eof加上
            buf.advance(eof_len);
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(last_err.unwrap_or_else(|| {
                Error::new(ErrorKind::Other, "all poll read failed")
            })))
        }
    }
}
