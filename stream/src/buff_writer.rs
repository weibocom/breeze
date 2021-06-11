use std::future::Future;
use std::io::{Error, Result};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use futures::ready;
use tokio::io::AsyncWrite;

unsafe impl<'a, T, W> Send for BuffCopyTo<'a, T, W> {}
unsafe impl<'a, T, W> Sync for BuffCopyTo<'a, T, W> {}

pub trait Request {
    fn on_success(&self, n: usize);
    fn stream_fetch(&self) -> Option<&[u8]>;
    fn on_empty(&self, waker: Waker);
    fn on_error(&self, err: Error);
}

pub struct BuffCopyTo<'a, T, W> {
    // 一次poll_write没有写完时，会暂存下来
    left: Option<&'a [u8]>,
    pos: usize,
    bytes: usize,
    cond: &'a T,
    w: W,
}

impl<'a, T, W> BuffCopyTo<'a, T, W> {
    pub fn from(cond: &'a T, w: W) -> Self {
        Self {
            left: None,
            pos: 0,
            bytes: 0,
            cond: cond,
            w: w,
        }
    }
}

impl<'a, T, W> Future for BuffCopyTo<'a, T, W>
where
    W: AsyncWrite + Unpin,
    T: Request + Unpin,
{
    type Output = Result<usize>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        let mut writer = Pin::new(&mut me.w);
        loop {
            if let Some(left) = me.left.take() {
                while me.pos < left.len() {
                    let num = ready!(writer.as_mut().poll_write(cx, &left[me.pos..]))?;
                    debug_assert!(num > 0);
                    me.pos += num;
                    me.bytes += num;
                    me.cond.on_success(num);
                }
            }
            me.left = me.cond.stream_fetch();
            me.pos = 0;
            if me.left == None {
                me.cond.on_empty(cx.waker().clone());
                return Poll::Pending;
            }
        }
    }
}
