use std::io::Result;
/// 写入数据，并且同时写入到的follower/slaves, 但忽略follower的返回值。
/// 如果master写入失败，则请求直接返回。
/// 忽略所有follower的写入失败情况。
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use protocol::Protocol;

use crate::{AsyncNoReply, AsyncReadAll, AsyncWriteAll, Request, Response};

pub struct AsyncSetSync<M, F, P> {
    master: M,
    noreply: AsyncNoReply<F, P>,
}

impl<M, F, P> AsyncSetSync<M, F, P> {
    pub fn from_master(master: M, followers: Vec<F>, parser: P) -> Self {
        Self {
            master: master,
            noreply: AsyncNoReply::from(followers, parser),
        }
    }
}

impl<M, F, P> AsyncWriteAll for AsyncSetSync<M, F, P>
where
    M: AsyncWriteAll + Unpin,
    F: AsyncWriteAll + Unpin,
    P: Protocol,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        //log::debug!(" set req: {:?}", buf.data());
        let me = &mut *self;
        let ok = ready!(Pin::new(&mut me.master).poll_write(cx, buf));
        // 失败也同步数据。
        let _succ = me.noreply.try_poll_noreply(cx, buf);
        Poll::Ready(ok)
    }
}
impl<M, F, P> AsyncReadAll for AsyncSetSync<M, F, P>
where
    M: AsyncReadAll + Unpin,
    F: AsyncReadAll + Unpin,
    P: Protocol,
{
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        Pin::new(&mut self.master).poll_next(cx)
    }
}
impl<M, F, P> crate::Addressed for AsyncSetSync<M, F, P>
where
    M: crate::Addressed,
    F: crate::Addressed,
{
    fn addr(&self) -> crate::Address {
        self.master.addr()
    }
}
