use std::io::Result;
/// 写入数据，并且同时写入到的follower/slaves, 但忽略follower的返回值。
/// 如果master写入失败，则请求直接返回。
/// 忽略所有follower的写入失败情况。
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;

use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};

pub struct AsyncSetSync<M, W> {
    master: M,
    master_done: bool,
    // 所有往master写的数据，只需要往followers写，不需要读,
    // 要么当前请求是noreply的，要么由其他的Reader负责读取
    // 并且忽略所有返回结果即可。
    followers: Vec<W>,
    // 当前follower写入到的索引位置
    f_idx: usize,
}

impl<M, W> AsyncSetSync<M, W> {
    pub fn from_master(master: M, followers: Vec<W>) -> Self {
        Self {
            master: master,
            master_done: false,
            followers: followers,
            f_idx: 0,
        }
    }
}

//impl<M, W> AsyncWriteAll for AsyncSetSync<M, W> {}

impl<M, W> AsyncWriteAll for AsyncSetSync<M, W>
where
    M: AsyncWriteAll + Unpin,
    W: AsyncWriteAll + Unpin,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        if !me.master_done {
            ready!(Pin::new(&mut me.master).poll_write(cx, buf))?;
            me.master_done = true;
        }
        if me.followers.len() > 0 {
            while me.f_idx < me.followers.len() {
                let _ = ready!(
                    Pin::new(unsafe { me.followers.get_unchecked_mut(me.f_idx) })
                        .poll_write(cx, buf)
                )
                .map_err(|e| {
                    log::error!("write follower failed idx:{} err:{:?}", me.f_idx, e);
                });
                me.f_idx += 1;
            }
        }
        me.f_idx = 0;
        Poll::Ready(Ok(()))
    }
}
impl<M, W> AsyncReadAll for AsyncSetSync<M, W>
where
    M: AsyncReadAll + Unpin,
    W: Unpin,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        match Pin::new(&mut self.master).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(r) => {
                self.master_done = false;
                Poll::Ready(r)
            }
        }
    }
}
