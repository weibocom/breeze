/// 按一次获取多个key的请求时，类似memcache与redis的gets时，一种
/// 方式是把keys解析出来，然后分发分发给不同的shards；另外一种方
/// 式是把所有的keys发送给所有的后端，然后合并。这种方式没有keys
/// 的解析，会更加高效。适合到shards的分片不多的场景，尤其是一般
/// 一个keys的请求req通常只包含key，所以额外的load会比较低。
/// 发送给所有sharding的请求，有一个成功，即认定为成功。
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::backend::AddressEnable;
use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};
use protocol::Protocol;
use sharding::Sharding;

pub struct AsyncMultiGetSharding<S, P> {
    // 成功发送请求的shards
    statuses: Vec<Status>,
    shards: Vec<S>,
    parser: P,
    response: Option<Response>,
    servers: String, // TODO 目前仅仅用于一致性分析，分析完毕之后可以清理 fishermen
    shard_reqs: Option<Vec<(usize, Request)>>,
    alg: Sharding,
    err: Option<Error>,
}

impl<S, P> AsyncMultiGetSharding<S, P>
where
    S: AddressEnable,
{
    pub fn from_shard(shards: Vec<S>, p: P, hash: &str, d: &str) -> Self {
        let names = shards.iter().map(|s| s.get_address()).collect();
        let mut servers = "{".to_string();
        for s in &shards {
            servers += s.get_address().as_str();
            servers += ",";
        }
        servers += "}";

        Self {
            statuses: vec![Status::Init; shards.len()],
            shards: shards,
            shard_reqs: None,
            parser: p,
            response: None,
            servers,
            err: None,
            alg: Sharding::from(hash, d, names),
        }
    }

    pub fn reset(&mut self) {
        // 长度一般都非常小
        for status in self.statuses.iter_mut() {
            *status = Init;
        }
        self.shard_reqs.take();
    }
}

impl<S, P> AsyncWriteAll for AsyncMultiGetSharding<S, P>
where
    S: AsyncWriteAll + AddressEnable + Unpin,
    P: Unpin + Protocol,
{
    // 只要有一个shard成功就算成功,如果所有的都写入失败，则返回错误信息。
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, multi: &Request) -> Poll<Result<()>> {
        let mut me = &mut *self;
        if me.shard_reqs.is_none() {
            me.shard_reqs = Some(me.parser.sharding(multi, &me.alg));
        }
        //let shard_reqs = me.shard_reqs.as_mut().expect("multi get sharding");
        // debug_assert!(shard_reqs.len() > 0);

        let mut success = false;
        let mut pending = false;
        let mut noreply = false;
        let mut reqs_len = 0;
        if let Some(shard_reqs) = me.shard_reqs.as_mut() {
            reqs_len = shard_reqs.len();
            for (i, req) in shard_reqs.iter() {
                let sharding_idx = *i;
                if req.noreply() {
                    noreply = req.noreply();
                }

                debug_assert!(sharding_idx < me.statuses.len());
                let status = unsafe { me.statuses.get_unchecked_mut(sharding_idx) };
                if *status == Init {
                    match Pin::new(unsafe { me.shards.get_unchecked_mut(sharding_idx) })
                        .poll_write(cx, req)
                    {
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
        }
        // 如果是noreply的回种请求，发送完毕，请求则完毕，需要进行重制
        if noreply {
            me.reset();
        }

        if pending {
            Poll::Pending
        } else if success {
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(me.err.take().unwrap_or(Error::new(
                ErrorKind::NotFound,
                format!(
                    "sharding server({}) must be greater than 0. req sharding num({}) must be greater than 0. reqeust keys({}) must great than 0",
                    me.shards.len(),
                    reqs_len,
                    multi.keys().len()
                ),
            ))))
        }
    }
}

impl<S, P> AsyncReadAll for AsyncMultiGetSharding<S, P>
where
    S: AsyncReadAll + AddressEnable + Unpin,
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        let mut pending = false;
        let shard_reqs = me.shard_reqs.as_ref().expect("multi get sharding");
        for (i, _) in shard_reqs {
            let status = unsafe { me.statuses.get_unchecked_mut(*i) };
            if *status == Sent {
                let mut r = Pin::new(unsafe { me.shards.get_unchecked_mut(*i) });
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
            // for status in me.statuses.iter_mut() {
            //     *status = Init;
            // }
            // me.shard_reqs.take();
            me.reset();
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
