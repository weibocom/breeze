use std::pin::Pin;
use std::task::Context;

use protocol::Protocol;

use crate::{AsyncWriteAll, Request};

pub struct AsyncNoReply<S, P> {
    backends: Vec<S>,
    parser: P,
}

impl<S, P> AsyncNoReply<S, P> {
    pub fn from(backends: Vec<S>, parser: P) -> Self {
        Self { backends, parser }
    }
}

impl<S, P> AsyncNoReply<S, P>
where
    S: AsyncWriteAll + Unpin,
    P: Protocol,
{
    // 尝试一次poll。失败，pending均忽略。
    // 返回成功发送的数量
    #[inline(always)]
    pub fn try_poll_noreply(&mut self, cx: &mut Context, req: &Request) {
        self.try_poll_noreply_seq(cx, req, self.backends.len())
    }
    // 按顺序向num个backend回写noreply的数据
    #[inline]
    pub fn try_poll_noreply_seq(&mut self, cx: &mut Context, req: &Request, num: usize) {
        let me = &mut *self;
        if num < me.backends.len() {
            let noreply = me.to_noreply(req);
            for idx in 0..num {
                let w = unsafe { me.backends.get_unchecked_mut(idx) };
                let _ = Pin::new(w).poll_write(cx, &noreply);
            }
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
