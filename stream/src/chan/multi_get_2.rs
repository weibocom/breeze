// 封装multi_get.rs，当前的multi_get是单层访问策略，需要封装为多层
// TODO： 有2个问题：1）单层访问改多层，封装multiGetSharding? 2) 需要解析key。如果需要解析key，那multiGetSharding还有存在的价值吗？
// TODO：待和@icy 讨论。

pub struct AsyncMultiGet<L, P> {
    // 当前从哪个layer开始发送请求
    idx: usize,
    layers: Vec<L>,
    keys: Vec<String>,
    parser: P,
}

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::chan::AsyncWriteAll;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use futures::ready;

impl<L, P> AsyncWriteAll for AsyncMultiGet<L, P> {}

impl<L, P> AsyncMultiGet<L, P> {
    pub fn from_layers(layers: Vec<L>, p: P) -> Self {
        Self {
            idx: 0,
            // writes: vec![false; shards.len()],
            layers,
            keys: Vec::new(),
            parser: p,
        }
    }

    // 发送请求，如果失败，继续向下一层write，注意处理重入问题
    // ready! 会返回Poll，所以这里还是返回Poll了
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<usize>> {
        let mut idx = self.idx;

        debug_assert!(self.req_len > 0);
        debug_assert!(idx < self.layers.len());

        // 发送请求之前首先设置resp found为false
        self.resp_found = false;

        // let req = self.req_data();
        let ptr = self.req_ptr as *const u8;
        let data = unsafe { std::slice::from_raw_parts(ptr, self.req_len) };

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
        let ptr = self.req_ptr as *const u8;
        unsafe { std::slice::from_raw_parts(ptr, self.req_len) }
    }
}

impl<S, P> AsyncWrite for AsyncMultiGet<S, P>
where
    S: AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin,
{
    // 请求某一层
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {}
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        // 为了简单。flush时不考虑pending，即使从pending恢复，也可以把之前的poll_flush重新操作一遍。
        // poll_flush是幂等的
        let me = &mut *self;
        let mut last_err = None;
        for (i, &success) in me.writes.iter().enumerate() {
            if success {
                match ready!(Pin::new(unsafe { me.shards.get_unchecked_mut(i) }).poll_flush(cx)) {
                    Err(e) => last_err = Some(e),
                    _ => {}
                }
            }
        }
        match last_err {
            Some(e) => Poll::Ready(Err(e)),
            _ => Poll::Ready(Ok(())),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
        let me = &mut *self;
        let mut last_err = None;
        for (i, &success) in me.writes.iter().enumerate() {
            if success {
                match ready!(Pin::new(unsafe { me.shards.get_unchecked_mut(i) }).poll_shutdown(cx))
                {
                    Err(e) => last_err = Some(e),
                    _ => {}
                }
            }
        }
        match last_err {
            Some(e) => Poll::Ready(Err(e)),
            _ => Poll::Ready(Ok(())),
        }
    }
}

impl<S, P> AsyncRead for AsyncMultiGet<S, P>
where
    S: AsyncRead + Unpin,
    P: Unpin + crate::Protocol,
{
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut ReadBuf,
    ) -> Poll<Result<()>> {
        let me = &mut *self;
        let mut success = false;
        let mut last_err = None;
        let offset = me.idx;
        let mut eof_len = 0;
        for i in offset..me.shards.len() {
            if me.writes[i] {
                let mut r = Pin::new(unsafe { me.shards.get_unchecked_mut(i) });
                let mut c_done = false;
                while !c_done {
                    match r.as_mut().poll_read(cx, buf) {
                        Poll::Pending => {
                            // 如果有数据获取，则返回数据。触发下一次获取。
                            // 如果未获取到数据，则pending
                            return if buf.filled().len() == 0 {
                                Poll::Pending
                            } else {
                                Poll::Ready(Ok(()))
                            };
                        }
                        Poll::Ready(Err(e)) => {
                            c_done = true;
                            last_err = Some(e)
                        }
                        Poll::Ready(Ok(())) => {
                            success = true;
                            // 检查当前请求是否包含请求的结束
                            let filled = buf.filled().len();
                            let (done, n) = me.parser.probe_response_eof(buf.filled());
                            if done {
                                c_done = true;
                                eof_len = filled - n;
                                buf.set_filled(n);
                            }
                        }
                    }
                }
            }
            me.idx += 1;
        }
        me.idx = 0;
        if success {
            // 把最后一次成功请求的eof加上
            buf.advance(eof_len);
            Poll::Ready(Ok(()))
        } else {
            Poll::Ready(Err(last_err.unwrap_or_else(|| {
                Error::new(ErrorKind::Other, "all poll read failed")
            })))
        }
    }
}
