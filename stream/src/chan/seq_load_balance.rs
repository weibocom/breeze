use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};
use protocol::{Protocol, Request};
use crate::{Address, Addressed, AsyncReadAll, AsyncWriteAll, LayerRole, LayerRoleAble, Response};
use std::io::{Error, ErrorKind, Result};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release};

pub struct SeqLoadBalance<B, P> {
    // 该层在分层中的角色role
    role: LayerRole,
    seq: AtomicUsize,
    targets: Vec<B>,
    parser: P,
}

impl<B, P> SeqLoadBalance<B, P>
    where
        B: Addressed,
{
    pub fn from(
        role: LayerRole,
        targets: Vec<B>,
        parser: P,
    ) -> Self {
        Self {
            role,
            seq: AtomicUsize::from(0 as usize),
            targets,
            parser,
        }
    }
}

impl<B, P> AsyncWriteAll for SeqLoadBalance<B, P>
    where
        B: AsyncWriteAll + Unpin,
        P: Protocol + Unpin,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        let seq = me.seq.fetch_add(1, Release);
        let index = seq % me.targets.len();
        unsafe { Pin::new(me.targets.get_unchecked_mut(index)).poll_write(cx, buf) }
    }
}

impl<B, P> AsyncReadAll for SeqLoadBalance<B, P>
    where
        B: AsyncReadAll + Unpin,
        P: Protocol + Unpin,
{
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        let seq = me.seq.fetch_add(1, Release);
        let index = seq % me.targets.len();
        unsafe { Pin::new(me.targets.get_unchecked_mut(index)).poll_next(cx) }
    }
}

impl<B, P> Addressed for SeqLoadBalance<B, P>
    where
        B: Addressed,
{
    fn addr(&self) -> Address {
        self.targets.addr()
    }
}

impl<B, P> LayerRoleAble for SeqLoadBalance<B, P> {
    fn layer_role(&self) -> LayerRole {
        self.role.clone()
    }

    fn is_master(&self) -> bool {
        self.role == LayerRole::Master
    }
}
