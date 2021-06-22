// 对于mc，全部穿透顺序：首先读取L1，如果miss，则读取master，否则读取slave;
// 此处优化为：外部传入对应需要访问的层次，顺讯读取对应的每个层，读取miss，继续访问后续的层。
// 任何一层读取成功，如果前面读取过其他层，则在返回后，还需要进行回写操作。
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::chan::AsyncWriteAll;
use tokio::io::{self, AsyncRead, AsyncWrite, ReadBuf};

pub struct AsyncGetSync<R, P> {
    // 当前从哪个shard开始发送请求
    idx: usize,
    // 需要read though的shards
    readers: Vec<R>,
    req: Vec<u8>,
    parser: P,
}

impl<R, P> AsyncWriteAll for AsyncGetSync<R, P> {}
impl<R, P> AsyncGetSync<R, P> {
    pub fn from(readers: Vec<R>, p: P) -> Self {
        AsyncGetSync {
            idx: 0,
            readers,
            req: Vec::new(),
            parser: p,
        }
    }
}

impl<R, P> AsyncGetSync<R, P>
where
    R: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin,
{
    fn do_req(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        self.idx = self.idx + 1;
        let mut msg_prefix = "will retry - ";
        if self.idx == self.readers.len() {
            msg_prefix = "stop req -"
        }
        // 记录req
        self.req.reserve(buf.len());
        self.req.copy_from_slice(buf);

        let idx = self.idx;
        let rpool = unsafe { self.readers.get_unchecked_mut(idx) };
        match Pin::new(rpool).poll_write(cx, buf) {
            Poll::Pending => {
                println!(
                    "{}for writing msg to mc#{} failed for pending",
                    msg_prefix, idx
                );
                return Poll::Pending;
            }
            Poll::Ready(Err(e)) => {
                println!(
                    "{} for write msg to {} failed for err: {}",
                    msg_prefix, idx, e
                );
                return Poll::Ready(Err(e));
            }
            _ => {
                // 写成功
                return Poll::Ready(Ok(buf.len()));
            }
        }
    }
}

impl<R, P> AsyncWrite for AsyncGetSync<R, P>
where
    R: AsyncRead + AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin,
{
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize>> {
        return self.do_req(cx, buf);
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
        let idx = self.idx;
        let reader = unsafe { self.readers.get_unchecked_mut(idx) };
        match Pin::new(reader).poll_read(cx, buf) {
            Poll::Pending => {
                // TODO: 对于buf中有部分数据，是否合适？
                // 如果有数据获取，则返回数据,触发下一次获取。
                // 如果未获取到数据，则pending
                return if buf.filled().len() == 0 {
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                };
            }
            Poll::Ready(Err(e)) => {
                println!("found err in get: {:?}", e);
            }
            // 如果接受到数据，检测是否是命中，只有命中，请求才算完毕
            Poll::Ready(Ok(())) => {
                if self.parser.probe_response_succeed(buf.filled()) {
                    return Poll::Ready(Ok(()));
                }
            }
        }
        // 到了这里，说明请求没有get到结果，需要继续穿透访问
        if self.idx + 1 < self.readers.len() {
            self.idx += 1;
            let req = self.req.clone();
            let msg_prefix = "rewrite when miss -";
            match self.do_req(cx, req.as_slice()) {
                Poll::Pending => {
                    println!(
                        "{}for writing msg to mc#{} failed for pending",
                        msg_prefix, idx
                    );
                    return Poll::Pending;
                }
                Poll::Ready(Err(e)) => {
                    println!(
                        "{} for write msg to {} failed for err: {}",
                        msg_prefix, idx, e
                    );
                    return Poll::Ready(Err(e));
                }
                _ => {
                    // 写成功
                    return Poll::Ready(Ok(()));
                }
            }
        }

        Poll::Ready(Ok(()))
    }
}
