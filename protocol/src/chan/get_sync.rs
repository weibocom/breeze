// 对于mc，全部穿透顺序：首先读取L1，如果miss，则读取master，否则读取slave;
// 此处优化为：外部传入对应需要访问的层次，顺讯读取对应的每个层，读取miss，继续访问后续的层。
// 任何一层读取成功，如果前面读取过其他层，则在返回后，还需要进行回写操作。
use std::io::{self, Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use super::RequestRef;
use crate::chan::AsyncWriteAll;
use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub struct AsyncGetSync<R, P> {
    // 当前从哪个shard开始发送请求
    idx: usize,
    // 需要read though的layers
    layers: Vec<R>,
    req_ref: RequestRef,
    // TODO: 对于空响应，根据协议获得空响应格式，这样效率更高，待和@icy 讨论 fishermen 2021.6.27
    empty_resp: Vec<u8>,
    resp_found: bool,
    parser: P,
}

impl<R, P> AsyncWriteAll for AsyncGetSync<R, P> {}
impl<R, P> AsyncGetSync<R, P> {
    pub fn from(layers: Vec<R>, p: P) -> Self {
        AsyncGetSync {
            idx: 0,
            layers,
            req_ref: RequestRef::from(0, 0),
            empty_resp: Vec::new(),
            resp_found: false,
            parser: p,
        }
    }
}

impl<R, P> AsyncGetSync<R, P>
where
    R: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin,
{
    // 发送请求，如果失败，继续向下一层write，注意处理重入问题
    // ready! 会返回Poll，所以这里还是返回Poll了
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<usize>> {
        let mut idx = self.idx;

        debug_assert!(self.req_ref.validate());
        debug_assert!(idx < self.layers.len());

        // 发送请求之前首先设置resp found为false
        self.resp_found = false;

        // let req = self.req_data();

        let ptr = self.req_ref.ptr() as *const u8;
        let data = unsafe { std::slice::from_raw_parts(ptr, self.req_ref.len()) };

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
        self.empty_resp.clear();
    }

    // TODO: 使用这个方法，会导致借用问题，先留着
    fn req_data(&mut self) -> &[u8] {
        let ptr = self.req_ref.ptr() as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, self.req_ref.len()) }
    }
}

impl<R, P> AsyncWrite for AsyncGetSync<R, P>
where
    R: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize>> {
        // 记录req buf，方便多层访问
        self.req_ref = RequestRef::from(buf.as_ptr() as usize, buf.len());
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

impl<R, P> AsyncRead for AsyncGetSync<R, P>
where
    R: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin + crate::Protocol,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut me = &mut *self;
        // check precondition
        debug_assert!(me.idx < me.layers.len());

        // 注意重入问题，写成功后，如何回写？
        while me.idx < me.layers.len() {
            //for each
            let reader = unsafe { me.layers.get_unchecked_mut(me.idx) };
            match ready!(Pin::new(reader).poll_read(cx, buf)) {
                Ok(_) => {
                    // 请求命中，返回ok及消息长度；
                    if !me.resp_found {
                        if me.parser.probe_response_found(buf.filled()) {
                            me.resp_found = true;
                        }
                    }
                    // 请求命中，清理empty resp，并返回;如果是最后一个layer，直接返回；否则保留一份emtpy/special响应
                    if me.resp_found {
                        me.empty_resp.clear();
                        return Poll::Ready(Ok(()));
                    } else if me.idx + 1 == me.layers.len() {
                        me.empty_resp.clear();
                        return Poll::Ready(Ok(()));
                    } else if me.empty_resp.len() == 0 {
                        // TODO 对于空响应，由底层确认有足够数据
                        me.empty_resp.reserve(buf.capacity() - buf.remaining());
                        me.empty_resp.copy_from_slice(buf.filled());
                    }
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

            // 还有reader,继续重试后续的reader
            buf.clear();
            me.idx += 1;
            ready!(me.do_write(cx))?;
        }

        debug_assert!(self.idx + 1 == self.layers.len());
        // 只要有empty response，则优先返回empty resp
        if self.empty_resp.len() > 0 {
            buf.put_slice(self.empty_resp.as_slice());
            return Poll::Ready(Ok(()));
        }

        Poll::Ready(Err(Error::new(ErrorKind::NotFound, "not found key")))
    }
}
