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
        //log::debug!(" set req: {:?}", buf.data());
        let me = &mut *self;
        if !me.master_done {
            ready!(Pin::new(&mut me.master).poll_write(cx, buf))?;
            me.master_done = true;
            if me.followers.len() > 0 {
                let data = me.parser.with_noreply(buf);
                let mut noreply = Request::from_request(data, buf);
                noreply.set_noreply();
                me.noreply = Some(noreply);
            }
        }
        if me.followers.len() > 0 {
            let noreply = me.noreply.as_ref().unwrap();
            while me.f_idx < me.followers.len() {
                let w = Pin::new(unsafe { me.followers.get_unchecked_mut(me.f_idx) });
                if let Err(_e) = ready!(w.poll_write(cx, noreply)) {
                    log::debug!("write follower failed idx:{} err:{:?}", me.f_idx, _e);
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
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        let response = ready!(Pin::new(&mut me.master).poll_next(cx))?;

        me.f_idx = 0;
        me.master_done = false;
        me.noreply.take();

        Poll::Ready(Ok(response))
    }
}
