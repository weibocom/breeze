// 封装multi_get.rs，当前的multi_get是单层访问策略，需要封装为多层
// TODO： 有2个问题：1）单层访问改多层，封装multiGetSharding? 2) 需要解析key。如果需要解析key，那multiGetSharding还有存在的价值吗？
// 分两步：1）在multi get中，解析多个cmd/key 以及对应的response，然后多层穿透访问；
//        2）将解析req迁移到pipelineToPingPong位置,同时改造req buf。

use std::collections::HashMap;

const MULTI_GET_SHRINK_SIZE: u32 = 2048;

pub struct AsyncMultiGet<L, P> {
    // 当前从哪个layer开始发送请求
    idx: usize,
    layers: Vec<L>,
    // origin_cmds: Slice,
    current_cmds: Slice,
    //left_cmds: Vec<Slice>,
    //key_values: HashMap<String, Response>,
    response: Option<Response>,
    parser: P,
}

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Response};
use ds::Slice;
use protocol::Protocol;
use tokio::io::ReadBuf;

use futures::ready;

impl<L, P> AsyncWriteAll for AsyncMultiGet<L, P> {
    fn from_layers(layers: Vec<L>, p: P) -> Self {
        Self {
            idx: 0,
            // writes: vec![false; shards.len()],
            layers,
            // origin_cmds: Default::default(),
            current_cmds: Default::default(),
            response: None,
            parser: p,
        }
    }

    // 发送请求，将current cmds发送到所有mc，如果失败，继续向下一层write，注意处理重入问题
    // ready! 会返回Poll，所以这里还是返回Poll了
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<usize>> {
        let mut idx = self.idx;

        debug_assert!(idx < self.layers.len());
        let data = self.current_cmds.data();

        // 当前layer的reader发送请求，直到发送成功
        while idx < self.layers.len() {
            let reader = unsafe { self.layers.get_unchecked_mut(idx) };
            match ready!(Pin::new(reader).poll_write(cx, data)) {
                Ok(_) => return Poll::Ready(Ok(())),
                Err(e) => {
                    self.idx += 1;
                    idx = self.idx;
                    log::debug!("write req failed e:{:?}", e);
                }
            }
        }

        // write req到所有资源失败，reset并返回err
        self.reset();
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "cannot write multi-reqs to all resources",
        )))
    }

    // 请求处理完毕，进行reset
    fn reset(&mut self) {
        self.idx = 0;
        // try shrink left cmds
        if self.left_cmds.len() > MULTI_GET_SHRINK_SIZE {
            self.left_cmds = Vec::new();
        } else {
            self.left_cmds.clear();
        }
        // try shrink left key values
        if self.key_values.len() > MULTI_GET_SHRINK_SIZE {
            self.key_values = Vec::new();
        } else {
            self.key_values.clear();
        }
    }
}

impl<L, P> AsyncWrite for AsyncMultiGet<L, P>
where
    L: AsyncWrite + AsyncWriteAll + Unpin,
    P: Unpin,
{
    // 请求某一层
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>> {
        self.current_cmds = Slice::from(buf);
        return self.do_write(cx);
    }
}

impl<L, P> AsyncReadAll for AsyncMultiGet<L, P>
where
    L: AsyncReadAll + AsyncWriteAll + Unpin,
    P: Unpin + Protocol,
{
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Response>> {
        let me = &mut *self;
        debug_assert!(self.idx < self.layers.len());
        let mut last_err = None;
        let mut req_cmds = Vec::new();
        let found_keys = Vec::new();
        while (me.idx < me.layers.len()) {
            // 重新构建request cmd
            if found_keys.len() > 0 {
                req_cmds = me.parser.remove_found_cmd(me.current_cmds, keys);
                let cmds = req_cmds.as_mut_slice();
                me.current_cmds = Slice::from(&cmds);
            }

            let layer = unsafe { me.layers.get_unchecked_mut(me.idx) };
            match ready!(Pin::new(layer).poll_next(cx)) {
                Ok(item) => {
                    // 读到响应，轮询出改respons的noop及查到的keys
                    // TODO：方案1：noop作为标准response返回，这样需要在解析时，忽略noop
                    //       方案2：noop在正常的respons中，由外层进行过滤剔除
                    // 方案待定，暂时先倾向于方案1 fishermen
                    found_keys.clear();
                    let (found_keys, noop_pos) = me.parser.scan_response(&item, found_keys);
                    match me.response {
                        Some(all_resps) => all_resps.append(item),
                        None => me.response = Some(item),
                    }
                }
                Err(e) => last_err = Some(e),
            }
            m.idx += 1;
        }

        me.reset();
        me.response
            .take()
            .map(|item| Poll::Ready(Ok(item)))
            .unwrap_or_else(|| {
                Poll::Ready(Err(last_err.unwrap_or_else(|| {
                    Error::new(ErrorKind::Other, "all poll read failed")
                })))
            })
    }
}
