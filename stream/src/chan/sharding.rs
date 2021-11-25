use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use protocol::Protocol;

use crate::{
    Address, Addressed, AsyncReadAll, AsyncWriteAll, LayerRole, LayerRoleAble, Names, Request,
    Response,
};

use crate::Sharding;

pub struct AsyncSharding<B, P> {
    // 该层在分层中的角色role
    role: LayerRole,
    idx: usize,
    // 用于modula分布以及一致性hash定位server
    shards: Vec<B>,
    policy: Sharding,
    parser: P,
}

impl<B, P> AsyncSharding<B, P>
where
    B: Addressed,
{
    pub fn from(
        role: LayerRole,
        shards: Vec<B>,
        hash: &str,
        distribution: &str,
        parser: P,
    ) -> Self {
        let idx = 0;
        let policy = Sharding::from(hash, distribution, shards.names());
        Self {
            role,
            idx,
            shards,
            policy,
            parser,
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
        me.idx = me.policy.sharding(&key);
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
        log::debug!("key = {}, goto idx {}", String::from_utf8(key.data().to_vec()).unwrap(), me.idx);
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

impl<B, P> Addressed for AsyncSharding<B, P>
where
    B: Addressed,
{
    fn addr(&self) -> Address {
        self.shards.addr()
    }
}

impl<B, P> LayerRoleAble for AsyncSharding<B, P> {
    fn layer_role(&self) -> LayerRole {
        self.role.clone()
    }

    fn is_master(&self) -> bool {
        self.role == LayerRole::Master
    }
}
