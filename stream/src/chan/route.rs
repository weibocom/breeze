use std::collections::HashMap;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};
use protocol::Operation;

/// 这个只支持ping-pong请求。将请求按照固定的路由策略分发到不同的dest
/// 并且AsyncOpRoute的buf必须包含一个完整的请求。
pub struct AsyncOpRoute<B> {
    backends: Vec<B>,
    idx: usize,
}

impl<B> AsyncOpRoute<B> {
    pub fn from(op_routes: HashMap<Operation, B>) -> Self
    where
        B: AsyncWriteAll + Unpin,
    {
        let mut backends: Vec<(Operation, B)> =
            op_routes.into_iter().map(|(op, b)| (op, b)).collect();
        backends.sort_by(|a, b| (a.0 as u8).cmp(&(b.0 as u8)));
        let backends = backends
            .into_iter()
            .enumerate()
            .map(|(i, (op, b))| {
                assert_eq!(i, op as usize);
                b
            })
            .collect();
        let idx = 0;
        Self { backends, idx }
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
        me.idx = buf.operation() as usize;
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
