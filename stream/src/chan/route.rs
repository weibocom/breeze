use std::collections::HashMap;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};
use protocol::{Operation, OPERATION_NUM};

/// 这个只支持ping-pong请求。将请求按照固定的路由策略分发到不同的dest
/// 并且AsyncOpRoute的buf必须包含一个完整的请求。
pub struct AsyncOpRoute<B> {
    backends: Vec<B>,
    idx: usize,
    route: [u8; OPERATION_NUM],
}

impl<B> AsyncOpRoute<B> {
    pub fn from(op_routes: HashMap<Operation, B>, alias: HashMap<Operation, Operation>) -> Self
    where
        B: AsyncWriteAll + Unpin,
    {
        let mut backends = Vec::with_capacity(op_routes.len());
        let mut i = 0;
        let mut route = [0u8; OPERATION_NUM];
        for (op, backend) in op_routes.into_iter() {
            route[op as usize] = i;
            backends.push(backend);
            i += 1;
        }
        for (op, alias) in alias.into_iter() {
            route[op as usize] = route[alias as usize];
        }
        let idx = 0;
        Self {
            backends,
            idx,
            route,
        }
    }
}

impl<B> AsyncWriteAll for AsyncOpRoute<B>
where
    B: AsyncWriteAll + Unpin,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        // ping-pong请求，有写时，read一定是读完成了
        me.idx = me.route[buf.operation() as usize] as usize;
        debug_assert!(me.idx < me.backends.len());
        unsafe { Pin::new(me.backends.get_unchecked_mut(me.idx)).poll_write(cx, buf) }
    }
}

impl<B> AsyncReadAll for AsyncOpRoute<B>
where
    B: AsyncReadAll + Unpin,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        unsafe { Pin::new(me.backends.get_unchecked_mut(me.idx)).poll_next(cx) }
    }
}

use crate::{Address, Addressed};
impl<B> Addressed for AsyncOpRoute<B>
where
    B: Addressed,
{
    fn addr(&self) -> Address {
        self.backends.addr()
    }
}
