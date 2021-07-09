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
    _parser: P,
    response: Option<Response>,
    eof: usize,
}

use std::io::{Error, ErrorKind, Result};
use std::ops::Deref;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};
use protocol::Protocol;

use futures::ready;

impl<S, P> AsyncMultiGetSharding<S, P> {
    pub fn from_shard(shards: Vec<S>, p: P) -> Self {
        Self {
            idx: 0,
            writes: vec![false; shards.len()],
            shards: shards,
            _parser: p,
            response: None,
            eof: 0,
        }
    }
}

impl<S, P> AsyncWriteAll for AsyncMultiGetSharding<S, P>
where
    S: AsyncWriteAll + Unpin,
    P: Unpin,
{
    // 只要有一个shard成功就算成功,如果所有的都写入失败，则返回错误信息。
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        log::debug!("multi get poll write received.");
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
            log::debug!("========== write req: {:?}", buf.deref().data());
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(last_err.unwrap_or_else(|| {
                Error::new(ErrorKind::Other, "no request sent.")
            })))
        }
    }
}

impl<S, P> AsyncReadAll for AsyncMultiGetSharding<S, P>
where
    S: AsyncReadAll + Unpin,
    P: Unpin + Protocol,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        let mut last_err = None;
        let offset = me.idx;
        log::debug!(
            "+++++++=========== in getMulti layer: {}, all:{}",
            offset,
            me.shards.len()
        );
        for i in offset..me.shards.len() {
            log::debug!(
                "+++++++=========== in getMulti layer writed:{}",
                me.writes[i]
            );
            if me.writes[i] {
                let mut r = Pin::new(unsafe { me.shards.get_unchecked_mut(i) });
                match ready!(r.as_mut().poll_next(cx)) {
                    Err(e) => {
                        log::debug!("+++++++layer - err:{:?}", e);
                        last_err = Some(e);
                    }
                    Ok(response) => match me.response.as_mut() {
                        Some(item) => {
                            log::debug!(
                                "+++++++layer - found resp for multi-get, len:{}",
                                item.items.len()
                            );
                            // 之前的response中包含一个noop结尾消息，需要去掉
                            // item.cut_tail(me._parser.tail_size_for_multi_get());
                            item.append(response);
                        }
                        None => {
                            log::debug!(
                                "+++++++layer - first found resp for multi-get, len:{}",
                                response.items.len()
                            );
                            me.response = Some(response);
                        }
                    },
                }
            }
            me.idx += 1;
        }
        me.idx = 0;
        me.eof = 0;
        me.response
            .take()
            .map(|item| {
                log::debug!("+++++++ response item size: {}", item.items.len());
                return Poll::Ready(Ok(item));
            })
            .unwrap_or_else(|| {
                Poll::Ready(Err(last_err.unwrap_or_else(|| {
                    Error::new(ErrorKind::Other, "all poll read failed in layer")
                })))
            })
    }
}
