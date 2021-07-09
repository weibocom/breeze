use std::io::Result;
/// 写入数据，并且同时写入到的follower/slaves, 但忽略follower的返回值。
/// 如果master写入失败，则请求直接返回。
/// 忽略所有follower的写入失败情况。
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use protocol::Protocol;

use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};

pub struct AsyncSetSync<M, F, P> {
    master: M,
    master_done: bool,
    // 所有往master写的数据，只需要往followers写，不需要读,
    // 要么当前请求是noreply的，要么由其他的Reader负责读取
    // 并且忽略所有返回结果即可。
    followers: Vec<F>,
    // 当前follower写入到的索引位置
    f_idx: usize,
    parser: P,
    noreply: Option<Request>,
}

impl<M, F, P> AsyncSetSync<M, F, P> {
    pub fn from_master(master: M, followers: Vec<F>, parser: P) -> Self {
        Self {
            master: master,
            master_done: false,
            followers: followers,
            f_idx: 0,
            parser: parser,
            noreply: None,
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
        let me = &mut *self;
        if !me.master_done {
            ready!(Pin::new(&mut me.master).poll_write(cx, buf))?;
            me.master_done = true;
            if me.followers.len() > 0 {
                me.noreply = Some(me.parser.copy_noreply(buf));
            }
        }
        if me.followers.len() > 0 {
            let noreply = me.noreply.as_ref().unwrap();
            log::debug!("set: noreply:{}", noreply.noreply());
            while me.f_idx < me.followers.len() {
                let w = Pin::new(unsafe { me.followers.get_unchecked_mut(me.f_idx) });
                if let Err(e) = ready!(w.poll_write(cx, noreply)) {
                    log::warn!("write follower failed idx:{} err:{:?}", me.f_idx, e);
                }
                me.f_idx += 1;
            }
        }
        me.f_idx = 0;
        Poll::Ready(Ok(()))
    }
}
impl<M, F, P> AsyncReadAll for AsyncSetSync<M, F, P>
where
    M: AsyncReadAll + Unpin,
    F: AsyncReadAll + Unpin,
    P: Protocol,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        let has_response = match &me.noreply {
            None => true, // 没有，说明发送的不是noreply请求
            Some(req) => !req.noreply(),
        };
        if has_response {
            while me.f_idx < me.followers.len() {
                let r = Pin::new(unsafe { me.followers.get_unchecked_mut(me.f_idx) });
                if let Err(e) = ready!(r.poll_next(cx)) {
                    log::error!("set_sync: poll followers failed.{:?}", e);
                }
                me.f_idx += 1;
            }
        }
        me.f_idx = 0;

        let response = ready!(Pin::new(&mut self.master).poll_next(cx))?;
        self.master_done = false;
        Poll::Ready(Ok(response))
    }
}
