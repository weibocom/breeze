use std::pin::Pin;
use std::task::{Context, Poll};

use protocol::Protocol;

use crate::{AsyncWriteAll, Request};

pub struct AsyncNoReply<S, P> {
    backends: Vec<S>,
    parser: P,
    idx: usize,
}

impl<S, P> AsyncNoReply<S, P> {
    pub fn from(backends: Vec<S>, parser: P) -> Self {
        let idx = 0;
        Self {
            backends,
            parser,
            idx,
        }
    }
}

impl<S, P> AsyncNoReply<S, P>
where
    S: AsyncWriteAll + Unpin,
    P: Protocol,
{
    // 尝试一次poll。失败，pending均忽略。
    // 返回成功发送的数量
    pub fn try_poll_noreply(&mut self, cx: &mut Context, req: &Request) -> usize {
        let me = &mut *self;
        let mut succ = 0;
        if me.backends.len() > 0 {
            let noreply = me.to_noreply(req);
            for w in me.backends.iter_mut() {
                if let Poll::Ready(Ok(_)) = Pin::new(w).poll_write(cx, &noreply) {
                    succ += 1;
                }
            }
        }
        succ
    }
    // 随机选择一个backend，进行回种
    pub fn try_poll_noreply_one<A>(&mut self, cx: &mut Context, req: &Request) {
        let me = &mut *self;
        if me.backends.len() > 0 {
            let idx = (me.idx + 1) % me.backends.len();
            let noreply = me.to_noreply(req);
            let w = unsafe { me.backends.get_unchecked_mut(idx - 1) };
            let _ = Pin::new(w).poll_write(cx, &noreply);
        }
    }

    #[inline(always)]
    fn to_noreply(&self, req: &Request) -> Request {
        if req.noreply() {
            req.clone()
        } else {
            let data = self.parser.with_noreply(req);
            let mut noreply = Request::from_request(data, req.keys().clone().into(), req);
            noreply.set_noreply();
            noreply
        }
    }
}
