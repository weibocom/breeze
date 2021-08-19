use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::backend::AddressEnable;
use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};

use crate::Sharding;

pub struct AsyncSharding<B> {
    idx: usize,
    // 用于modula分布以及一致性hash定位server
    shards: Vec<B>,
    policy: Sharding,
}

impl<B> AsyncSharding<B>
where
    B: AddressEnable,
{
    pub fn from(shards: Vec<B>, hash: &str, distribution: &str) -> Self {
        let idx = 0;
        let names = shards.iter().map(|s| s.get_address()).collect();
        let policy = Sharding::from(hash, distribution, names);
        Self {
            idx,
            shards,
            policy,
        }
    }
}

impl<B> AsyncWriteAll for AsyncSharding<B>
where
    B: AsyncWriteAll + Unpin,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.shards.len());
        me.idx = me.policy.sharding(buf.data());
        if me.idx >= me.shards.len() {
            return Poll::Ready(Err(Error::new(
                ErrorKind::NotConnected,
                format!(
                    "not connected, maybe topology not inited. index out of bounds. {} >= {}",
                    me.idx,
                    me.shards.len()
                ),
            )));
        }
        unsafe { Pin::new(me.shards.get_unchecked_mut(me.idx)).poll_write(cx, buf) }
    }
}

impl<B> AsyncReadAll for AsyncSharding<B>
where
    B: AsyncReadAll + Unpin,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        unsafe { Pin::new(me.shards.get_unchecked_mut(me.idx)).poll_next(cx) }
    }
}
