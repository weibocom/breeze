// 对于mc，全部穿透顺序：首先读取L1，如果miss，则读取master，否则读取slave;
// 此处优化为：外部传入对应需要访问的层次，顺讯读取对应的每个层，读取miss，继续访问后续的层。
// 任何一层读取成功，如果前面读取过其他层，则在返回后，还需要进行回写操作。
use std::io::{self, Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Response};
use futures::ready;
use protocol::Protocol;
use tokio::io::AsyncWrite;

pub struct AsyncGetSync<R, P> {
    // 当前从哪个shard开始发送请求
    idx: usize,
    // 需要read though的layers
    layers: Vec<R>,
    req_ref: RequestRef,
    // TODO: 对于空响应，根据协议获得空响应格式，这样效率更高，待和@icy 讨论 fishermen 2021.6.27
    empty_resp: Option<Response>,
    resp_found: bool,
    parser: P,
}

impl<R, P> AsyncWriteAll for AsyncGetSync<R, P> {}
impl<R, P> AsyncGetSync<R, P> {
    pub fn from(layers: Vec<R>, p: P) -> Self {
        AsyncGetSync {
            idx: 0,
            layers,
            req_ref: RequestRef::empty(),
            empty_resp: None,
            resp_found: false,
            parser: p,
        }
    }
}

impl<R, P> AsyncGetSync<R, P>
where
    R: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin,
{
    // 发送请求，如果失败，继续向下一层write，注意处理重入问题
    // ready! 会返回Poll，所以这里还是返回Poll了
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<usize>> {
        let mut idx = self.idx;

        //debug_assert!(self.req_ref.validate());
        debug_assert!(idx < self.layers.len());

        // 发送请求之前首先设置resp found为false
        self.resp_found = false;

        //let req = self.req_data();

        //let ptr = self.req_ref.ptr() as *const u8;
        //let data = unsafe { std::slice::from_raw_parts(ptr, self.req_ref.len()) };
        let data = self.req_ref.data();

        // 轮询reader发送请求，直到发送成功
        while idx < self.layers.len() {
            let reader = unsafe { self.layers.get_unchecked_mut(idx) };
            match ready!(Pin::new(reader).poll_write(cx, data)) {
                Ok(len) => return Poll::Ready(Ok(len)),
                Err(e) => {
                    self.idx += 1;
                    idx = self.idx;
                    println!("write req failed e:{:?}", e);
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
    fn reset(&mut self) {
        self.idx = 0;
        self.resp_found = false;
        //self.empty_resp.clear();
    }

    // TODO: 使用这个方法，会导致借用问题，先留着
    //fn req_data(&mut self) -> &[u8] {
    //    let ptr = self.req_ref.ptr() as *const u8;
    //    unsafe { std::slice::from_raw_parts(ptr, self.req_ref.len()) }
    //}
}

impl<R, P> AsyncWrite for AsyncGetSync<R, P>
where
    R: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        // 记录req buf，方便多层访问
        self.req_ref = RequestRef::from(buf);
        return self.do_write(cx);
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        // TODO 这个不需要，每次只用flush idx对应的reader 待和@icy 确认
        // if self.readers.len() == 1 {
        //     unsafe {
        //         return Pin::new(self.readers.get_unchecked_mut(0)).poll_flush(cx);
        //     }
        // }

        let idx = self.idx;
        debug_assert!(self.idx < self.layers.len());
        let reader = unsafe { self.layers.get_unchecked_mut(idx) };
        match ready!(Pin::new(reader).poll_flush(cx)) {
            Err(e) => return Poll::Ready(Err(e)),
            _ => return Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        // TODO 这个不需要，每次只用flush idx对应的reader 待和@icy 确认
        // if self.readers.len() == 1 {
        //     unsafe {
        //         return Pin::new(self.readers.get_unchecked_mut(0)).poll_shutdown(cx);
        //     }
        // }

        let idx = self.idx;
        debug_assert!(idx < self.layers.len());
        let reader = unsafe { self.layers.get_unchecked_mut(idx) };
        match ready!(Pin::new(reader).poll_shutdown(cx)) {
            Err(e) => return Poll::Ready(Err(e)),
            _ => return Poll::Ready(Ok(())),
        }
    }
}

impl<R, P> AsyncReadAll for AsyncGetSync<R, P>
where
    R: AsyncReadAll + AsyncWrite + AsyncWriteAll + Unpin,
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
                    if !me.resp_found {
                        if me.parser.response_found(&item) {
                            self.empty_resp.take();
                            return Poll::Ready(Ok(item));
                        }
                    }
                    me.empty_resp.insert(item);
                    // 如果请求未命中，则继续准备尝试下一个reader
                }
                // 请求失败，如果还有reader，需要继续尝试下一个reader
                Err(e) => {
                    println!("read found err: {:?}", e);
                }
            }
            // 如果所有reader尝试完毕，退出循环
            if me.idx + 1 >= me.layers.len() {
                break;
            }

            me.idx += 1;
            ready!(me.do_write(cx))?;
        }

        debug_assert!(self.idx + 1 == self.layers.len());
        if let Some(item) = self.empty_resp.take() {
            Poll::Ready(Ok(item))
        } else {
            Poll::Ready(Err(Error::new(ErrorKind::NotFound, "not found key")))
        }
    }
    //fn poll_done(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
    //    let me = &mut self;
    //    for i in 0..me.idx {
    //        unsafe { ready!(Pin::new(me.layers.get_unchecked_mut(i)).poll_done(cx))? };
    //    }
    //    self.idx = 0;
    //    Poll::Ready(Ok(()))
    //}
}

struct RequestRef {
    ptr: usize,
    len: usize,
}

impl RequestRef {
    fn from(data: &[u8]) -> Self {
        Self {
            ptr: data.as_ptr() as usize,
            len: data.len(),
        }
    }
    fn empty() -> Self {
        Self { ptr: 0, len: 0 }
    }
    fn data(&self) -> &[u8] {
        debug_assert!(self.ptr > 0);
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len) }
    }
}
