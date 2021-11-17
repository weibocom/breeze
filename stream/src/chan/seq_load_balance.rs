use crate::{Address, Addressed, AsyncReadAll, AsyncWriteAll, LayerRole, LayerRoleAble, Response};
use protocol::{Protocol, Request};
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

pub struct SeqLoadBalance<B> {
    // 该层在分层中的角色role
    role: LayerRole,
    idx: usize,
    seq: AtomicUsize,
    targets: Vec<B>,
}

impl<B> SeqLoadBalance<B>
    where
        B: Addressed,
{
    pub fn from(role: LayerRole, targets: Vec<B>) -> Self {
        Self {
            role,
            seq: AtomicUsize::from(0 as usize),
            idx: 0 as usize,
            targets,
        }
    }
}

impl<B> AsyncWriteAll for SeqLoadBalance<B>
    where
        B: Addressed + AsyncWriteAll + Unpin,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        let seq = me.seq.fetch_add(1, Release);
        me.idx = seq % me.targets.len();
        log::debug!(
            "load balance sequence = {}, address: {}",
            me.idx,
            me.targets.get(me.idx).unwrap().addr().to_string()
        );
        unsafe { Pin::new(me.targets.get_unchecked_mut(me.idx)).poll_write(cx, buf) }
    }
}

impl<B> AsyncReadAll for SeqLoadBalance<B>
    where
        B: AsyncReadAll + Unpin,
{
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        unsafe { Pin::new(me.targets.get_unchecked_mut(me.idx)).poll_next(cx) }
    }
}

impl<B> Addressed for SeqLoadBalance<B>
    where
        B: Addressed,
{
    fn addr(&self) -> Address {
        self.targets.addr()
    }
}

impl<B> LayerRoleAble for SeqLoadBalance<B> {
    fn layer_role(&self) -> LayerRole {
        self.role.clone()
    }

    fn is_master(&self) -> bool {
        self.role == LayerRole::Master
    }
}