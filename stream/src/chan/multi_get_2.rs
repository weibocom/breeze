// 封装multi_get.rs，当前的multi_get是单层访问策略，需要封装为多层
// TODO： 有2个问题：1）单层访问改多层，封装multiGetSharding? 2) 需要解析key。如果需要解析key，那multiGetSharding还有存在的价值吗？
// 分两步：1）在multi get中，解析多个cmd/key 以及对应的response，然后多层穿透访问；
//        2）将解析req迁移到pipelineToPingPong位置,同时改造req buf。
pub struct AsyncMultiGet<L, P> {
    // 当前从哪个layer开始发送请求
    idx: usize,
    layers: Vec<L>,
    request_ref: Request,
    response: Option<Response>,
    parser: P,
}

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Response};
use protocol::{Protocol, Request};

use futures::ready;

impl<L, P> AsyncMultiGet<L, P>
where
    L: AsyncWriteAll + AsyncWriteAll + Unpin,
    P: Unpin,
{
    pub fn from_layers(layers: Vec<L>, p: P) -> Self {
        Self {
            idx: 0,
            // writes: vec![false; shards.len()],
            layers,
            // origin_cmds: Default::default(),
            request_ref: Default::default(),
            response: None,
            parser: p,
        }
    }

    // 发送请求，将current cmds发送到所有mc，如果失败，继续向下一层write，注意处理重入问题
    // ready! 会返回Poll，所以这里还是返回Poll了
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut idx = self.idx;
        debug_assert!(idx < self.layers.len());

        // 当前layer的reader发送请求，直到发送成功
        while idx < self.layers.len() {
            let reader = unsafe { self.layers.get_unchecked_mut(idx) };
            match ready!(Pin::new(reader).poll_write(cx, &self.request_ref)) {
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
        self.response = None;
    }
}

impl<L, P> AsyncWriteAll for AsyncMultiGet<L, P>
where
    L: AsyncWriteAll + AsyncWriteAll + Unpin,
    P: Unpin,
{
    // 请求某一层
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, req: &Request) -> Poll<Result<()>> {
        self.request_ref = req.clone();
        return self.do_write(cx);
    }
}

impl<L, P> AsyncReadAll for AsyncMultiGet<L, P>
where
    L: AsyncReadAll + AsyncWriteAll + Unpin,
    P: Unpin + Protocol,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.layers.len());
        let mut last_err = None;
        let mut found_keys = Vec::new();
        let mut new_req_data: Vec<u8> = Vec::new();

        while me.idx < me.layers.len() {
            log::debug!(" =========== get-multi loop:{}", me.idx);
            let layer = unsafe { me.layers.get_unchecked_mut(me.idx) };
            match ready!(Pin::new(layer).poll_next(cx)) {
                Ok(item) => {
                    // 轮询出已经查到的keys
                    found_keys.clear();
                    me.parser.scan_response_keys(&item, &mut found_keys);
                    match me.response.as_mut() {
                        Some(response) => response.append(item),
                        None => me.response = Some(item),
                    }
                }
                Err(e) => {
                    log::warn!(" ++++++++= get-multi err:{:?}", e);
                    last_err = Some(e);
                }
            }

            me.idx += 1;
            if me.idx >= me.layers.len() {
                break;
            }

            // 重新构建request cmd，再次请求
            if found_keys.len() > 0 {
                new_req_data.clear();
                me.parser.rebuild_get_multi_request(
                    &me.request_ref,
                    &found_keys,
                    &mut new_req_data,
                );
                // me.request_ref =
                //     Request::from(new_req_data.as_slice(), me.request_ref.id().clone());
                me.request_ref.update_data(new_req_data.as_slice());
            }

            match ready!(me.do_write(cx)) {
                Ok(()) => continue,
                Err(e) => {
                    log::warn!("found err when send layer request:{:?}", e);
                    last_err = Some(e);
                    break;
                }
            }
        }

        // 先拿走response，然后重置，最后返回响应列表
        let response = me.response.take();
        // 请求完毕，重置
        me.reset();
        response
            .map(|item| Poll::Ready(Ok(item)))
            .unwrap_or_else(|| {
                Poll::Ready(Err(last_err.unwrap_or_else(|| {
                    Error::new(ErrorKind::Other, "all poll read failed")
                })))
            })
    }
}
