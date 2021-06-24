// 对于mc，全部穿透顺序：首先读取L1，如果miss，则读取master，否则读取slave;
// 此处优化为：外部传入对应需要访问的层次，顺讯读取对应的每个层，读取miss，继续访问后续的层。
// 任何一层读取成功，如果前面读取过其他层，则在返回后，还需要进行回写操作。
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::chan::AsyncWriteAll;
use futures::ready;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};

pub struct AsyncGetSync<R, P> {
    // 当前从哪个shard开始发送请求
    idx: usize,
    // 需要read though的shards
    readers: Vec<R>,
    last_req: Vec<u8>,
    last_status: bool,
    parser: P,
}

impl<R, P> AsyncWriteAll for AsyncGetSync<R, P> {}
impl<R, P> AsyncGetSync<R, P> {
    pub fn from(readers: Vec<R>, p: P) -> Self {
        AsyncGetSync {
            idx: 0,
            readers,
            last_req: Vec::new(),
            last_status: false,
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
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<usize>> {
        // check idx
        let mut idx = self.idx;
        debug_assert!(idx < self.readers.len());
        if idx >= self.readers.len() {
            println!("idx/{} is exceed readers.len/{}", idx, self.readers.len());
            return Poll::Ready(Err(Error::new(ErrorKind::Interrupted, "idx is over range")));
        }

        // 轮询reader，发送请求
        while idx < self.readers.len() {
            let rpool = unsafe { self.readers.get_unchecked_mut(idx) };
            match ready!(Pin::new(rpool).poll_write(cx, self.last_req.as_slice())) {
                Ok(len) => return Poll::Ready(Ok(len)),
                Err(e) => {
                    self.idx += 1;
                    idx = self.idx;
                    println!("write req err: {:?}", e);
                }
            }
        }

        // 重置，以准备迎接下一个请求
        self.reset();

        // write req到所有资源失败
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "cannot write req to all resources",
        )))
    }

    // 请求完毕（成功or失败）后，做清理，准备迎接下一个请求
    fn reset(&mut self) {
        self.last_status = false;
        self.last_req.resize(0, 0);
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
        // 第一次请求时，记录req
        if self.last_req.len() == 0 {
            self.last_req.reserve(buf.len());
            self.last_req.copy_from_slice(buf);
        }
        return self.do_write(cx);
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let idx = self.idx;
        let reader = unsafe { self.readers.get_unchecked_mut(idx) };
        match Pin::new(reader).poll_flush(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            _ => return Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let idx = self.idx;
        let reader = unsafe { self.readers.get_unchecked_mut(idx) };
        match Pin::new(reader).poll_shutdown(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
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
        let mut idx = me.idx;
        debug_assert!(idx < me.readers.len());
        if idx >= me.readers.len() {
            println!("idx/{} over range, readers/{}", idx, me.readers.len());
            return Poll::Ready(Err(Error::new(ErrorKind::Interrupted, "idx is over range")));
        }

        // 注意重入问题
        while idx < me.readers.len() {
            let reader = unsafe { me.readers.get_unchecked_mut(idx) };
            match ready!(Pin::new(reader).poll_read(cx, buf)) {
                Ok(_) => {
                    if me.last_status {
                        // 对于请求成功并重入，直接返回
                        return Poll::Ready(Ok(()));
                    } else if me.parser.probe_response_found(buf.filled()) {
                        // 请求获取命中，设置status，并返回
                        me.last_status = true;
                        return Poll::Ready(Ok(()));
                    }
                    // 如果请求未命中，则继续准备尝试下一个reader
                }
                // 请求失败，如果还有reader，需要继续尝试下一个reader
                Err(e) => {
                    println!("read found err: {:?}", e);
                }
            }
            // 如果所有reader尝试完毕，退出循环
            if idx + 1 >= me.readers.len() {
                break;
            }

            // 还有reader,继续重试后续的reader
            buf.clear();
            me.idx += 1;
            idx = me.idx;
            match ready!(me.do_write(cx)) {
                Ok(len) => {
                    println!("write req len/{}", len);
                    continue;
                }
                Err(e) => {
                    // 发送消息到readers全部失败，结束
                    println!("write req failed, e:{:?}", e);
                    break;
                }
            }
        }

        debug_assert!(idx == self.readers.len());
        Poll::Ready(Err(Error::new(ErrorKind::NotFound, "not found key")))
    }
}
