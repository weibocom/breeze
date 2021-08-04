// 对于mc，全部穿透顺序：首先读取L1，如果miss，则读取master，否则读取slave;
// 此处优化为：外部传入对应需要访问的层次，顺讯读取对应的每个层，读取miss，继续访问后续的层。
// 任何一层读取成功，如果前面读取过其他层，则在返回后，还需要进行回写操作。
use std::io::{self, Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};
use futures::ready;
use protocol::Protocol;

pub struct AsyncGetSync<R, P> {
    // 当前从哪个shard开始发送请求
    idx: usize,
    // 需要read though的layers
    layers: Vec<R>,
    req: Request,
    // TODO: 对于空响应，根据协议获得空响应格式，这样效率更高，待和@icy 讨论 fishermen 2021.6.27
    empty_resp: Option<Response>,
    parser: P,
}

impl<R, P> AsyncGetSync<R, P> {
    pub fn from(layers: Vec<R>, p: P) -> Self {
        AsyncGetSync {
            idx: 0,
            layers,
            req: Default::default(),
            empty_resp: None,
            parser: p,
        }
    }
}

impl<R, P> AsyncGetSync<R, P>
where
    R: AsyncWriteAll + Unpin,
    P: Unpin,
{
    // 发送请求，如果失败，继续向下一层write，注意处理重入问题
    // ready! 会返回Poll，所以这里还是返回Poll了
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        //debug_assert!(idx < self.layers.len());

        // 轮询reader发送请求，直到发送成功
        while self.idx < self.layers.len() {
            let reader = unsafe { self.layers.get_unchecked_mut(self.idx) };
            match ready!(Pin::new(reader).poll_write(cx, &self.req)) {
                Ok(_) => return Poll::Ready(Ok(())),
                Err(_e) => {
                    self.idx += 1;
                    log::warn!("get_sync:write req failed e:{:?}", _e);
                }
            }
        }

        // write req到所有资源失败，reset并返回err
        self.reset();
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "cannot write req to all resources",
        )))
    }

    // 请求处理完毕，进行reset
    #[inline(always)]
    fn reset(&mut self) {
        self.idx = 0;
    }
}

impl<R, P> AsyncWriteAll for AsyncGetSync<R, P>
where
    R: AsyncWriteAll + Unpin,
    P: Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &Request,
    ) -> Poll<Result<()>> {
        // 记录req buf，方便多层访问
        self.req = buf.clone();
        self.do_write(cx)
    }
}

impl<R, P> AsyncReadAll for AsyncGetSync<R, P>
where
    R: AsyncReadAll + AsyncWriteAll + Unpin,
    P: Unpin + Protocol,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<Response>> {
        let mut me = &mut *self;
        // check precondition
        debug_assert!(me.idx < me.layers.len());

        // 注意重入问题，写成功后，如何回写？
        while me.idx < me.layers.len() {
            //for each
            let reader = unsafe { me.layers.get_unchecked_mut(me.idx) };
            match ready!(Pin::new(reader).poll_next(cx)) {
                Ok(item) => {
                    // 请求命中，返回ok及消息长度；
                    if me.parser.response_found(&item) {
                        self.empty_resp.take();
                        self.reset();
                        return Poll::Ready(Ok(item));
                    }
                    me.empty_resp.replace(item);
                    // 如果请求未命中，则继续准备尝试下一个reader
                }
                // 请求失败，如果还有reader，需要继续尝试下一个reader
                Err(_e) => {
                    log::debug!("get_sync:read found err: {:?}", _e);
                }
            }
            me.idx += 1;
            if me.idx < me.layers.len() {
                if let Err(_e) = ready!(me.do_write(cx)) {
                    log::debug!("get_sync: write failed:{:?}", _e);
                    break;
                }
            }
        }

        debug_assert!(self.idx == self.layers.len());
        self.reset();
        if let Some(item) = self.empty_resp.take() {
            Poll::Ready(Ok(item))
        } else {
            Poll::Ready(Err(Error::new(ErrorKind::NotFound, "not found key")))
        }
    }
}
