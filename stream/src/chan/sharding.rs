use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use protocol::Protocol;

use crate::backend::AddressEnable;
use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};

use crate::Sharding;

pub struct AsyncSharding<B, P> {
    idx: usize,
    // 用于modula分布以及一致性hash定位server
    shards: Vec<B>,
    policy: Sharding,
    parser: P,
    // TODO 当前主要是为了一致性排查需要，后续可以干掉 fishermen
    servers: String,
}

impl<B, P> AsyncSharding<B, P>
where
    B: AddressEnable,
{
    pub fn from(shards: Vec<B>, hash: &str, distribution: &str, parser: P) -> Self {
        let idx = 0;
        let names = shards.iter().map(|s| s.get_address()).collect();
        let policy = Sharding::from(hash, distribution, names);

        let mut servers = String::from("{");
        for s in &shards {
            servers += s.get_address().as_str();
            servers += ",";
        }
        servers += "}";

        Self {
            idx,
            shards,
            policy,
            parser,
            servers,
        }
    }
}

impl<B, P> AsyncWriteAll for AsyncSharding<B, P>
where
    B: AsyncWriteAll + Unpin,
    P: Protocol + Unpin,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.shards.len());
        let key = me.parser.key(buf);
        me.idx = me.policy.sharding(key);
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

impl<B, P> AsyncReadAll for AsyncSharding<B, P>
where
    B: AsyncReadAll + Unpin,
    P: Protocol + Unpin,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        unsafe { Pin::new(me.shards.get_unchecked_mut(me.idx)).poll_next(cx) }
    }
}

impl<B, P> AddressEnable for AsyncSharding<B, P> {
    fn get_address(&self) -> String {
        self.servers.clone()
    }
}
