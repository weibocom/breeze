/// 按一次获取多个key的请求时，类似memcache与redis的gets时，一种
/// 方式是把keys解析出来，然后分发分发给不同的shards；另外一种方
/// 式是把所有的keys发送给所有的后端，然后合并。这种方式没有keys
/// 的解析，会更加高效。适合到shards的分片不多的场景，尤其是一般
/// 一个keys的请求req通常只包含key，所以额外的load会比较低。
/// 发送给所有sharding的请求，有一个成功，即认定为成功。

// TODO 这个文件改为 multi_get_sharding? 待和@icy 确认 fishermen
// 思路： multi_get_sharding 是一种特殊的用于multi get 的shard读取，但支持单层，需要进一步封装为多层访问

pub struct AsyncMultiGetSharding<S, P> {
    // 成功发送请求的shards
    statuses: Vec<Status>,
    shards: Vec<S>,
    _parser: P,
    response: Option<Response>,
    servers: String, // TODO 目前仅仅用于一致性分析，分析完毕之后可以清理 fishermen
    err: Option<Error>,
}

use std::io::{Error, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::backend::AddressEnable;
use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};
use protocol::Protocol;

impl<S, P> AsyncMultiGetSharding<S, P>
where
    S: AddressEnable,
{
    pub fn from_shard(shards: Vec<S>, p: P) -> Self {
        let mut servers = String::from("{");
        for s in &shards {
            servers += s.get_address().as_str();
            servers += ",";
        }
        servers += "}";

        Self {
            statuses: vec![Status::Init; shards.len()],
            shards: shards,
            _parser: p,
            response: None,
            servers,
            err: None,
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
        let me = &mut *self;
        let mut success = false;
        let mut pending = false;
        for i in 0..me.shards.len() {
            let status = unsafe { me.statuses.get_unchecked_mut(i) };
            if *status == Init {
                match Pin::new(unsafe { me.shards.get_unchecked_mut(i) }).poll_write(cx, buf) {
                    Poll::Pending => pending = true,
                    Poll::Ready(Ok(_)) => {
                        success = true;
                        *status = Sent;
                    }
                    Poll::Ready(Err(e)) => {
                        *status = Error;
                        me.err = Some(e);
                    }
                }
            }
        }
        if pending {
            Poll::Pending
        } else if success {
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(me.err.take().unwrap()))
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
        let mut pending = false;
        for i in 0..me.shards.len() {
            let status = unsafe { me.statuses.get_unchecked_mut(i) };
            if *status == Sent {
                let mut r = Pin::new(unsafe { me.shards.get_unchecked_mut(i) });
                match r.as_mut().poll_next(cx) {
                    Poll::Pending => pending = true,
                    Poll::Ready(Ok(r)) => {
                        match me.response.as_mut() {
                            Some(exists) => exists.append(r),
                            None => me.response = Some(r),
                        };
                        *status = Done;
                    }
                    Poll::Ready(Err(e)) => {
                        *status = Error;
                        me.err = Some(e);
                    }
                }
            }
        }
        if pending {
            Poll::Pending
        } else {
            // 长度一般都非常小
            for status in me.statuses.iter_mut() {
                *status = Init;
            }
            me.response
                .take()
                .map(|item| Poll::Ready(Ok(item)))
                .unwrap_or_else(|| Poll::Ready(Err(me.err.take().unwrap())))
        }
    }
}

impl<S, P> AddressEnable for AsyncMultiGetSharding<S, P> {
    fn get_address(&self) -> String {
        self.servers.clone()
    }
}

#[repr(u8)]
#[derive(Clone, Copy)]
enum Status {
    Init,
    Sent,
    Done,
    Error,
}

impl PartialEq for Status {
    fn eq(&self, o: &Status) -> bool {
        *self as u8 == *o as u8
    }
}

use Status::*;
