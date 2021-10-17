use std::io::Result;
/// 写入数据，并且同时写入到的follower/slaves, 但忽略follower的返回值。
/// 如果master写入失败，则请求直接返回。
/// 忽略所有follower的写入失败情况。
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use protocol::{Operation, Protocol};

use crate::{AsyncNoReply, AsyncReadAll, AsyncWriteAll, Request, Response};

pub struct AsyncSetSync<M, F, P> {
    master: M,
    noreply: AsyncNoReply<F, P>,
    parser: P,
    request: Request,
    req_cas_add: bool,
}

impl<M, F, P> AsyncSetSync<M, F, P>
where
    P: Protocol,
{
    pub fn from_master(master: M, followers: Vec<F>, parser: P) -> Self {
        Self {
            master: master,
            parser: parser.clone(),
            noreply: AsyncNoReply::from(followers, parser),
            request: Default::default(),
            req_cas_add: false,
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

        // cas、add：只有对master发送后，且响应返回后才可以修改req为set并写请求。
        // 注：只处理单个request的cas、add，不支持setq、addq
        me.req_cas_add = me.parser.req_cas_or_add(buf);
        if me.req_cas_add {
            me.request = buf.clone();
            return Poll::Ready(ok);
        }

        // TODO: meta 使用了store的路由，此处需要提前返回，是否合适？待和icy讨论 fishermen
        if buf.operation() != Operation::Store {
            return Poll::Ready(ok);
        }
        // 失败也同步数据。
        let _succ = me.noreply.try_poll_noreply(cx, buf);
        Poll::Ready(ok)
    }
}
impl<M, F, P> AsyncReadAll for AsyncSetSync<M, F, P>
where
    M: AsyncReadAll + AsyncWriteAll + Unpin,
    F: AsyncReadAll + AsyncWriteAll + Unpin,
    P: Protocol,
{
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        match ready!(Pin::new(&mut me.master).poll_next(cx)) {
            Ok(rsp) => {
                if me.req_cas_add {
                    debug_assert!(me.request.len() > 0);
                    me.noreply.try_poll_noreply(cx, &me.request);
                }
                return Poll::Ready(Ok(rsp));
            }
            Err(err) => return Poll::Ready(Err(err)),
        }
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
