// 封装multi_get.rs，当前的multi_get是单层访问策略，需要封装为多层
// TODO： 有2个问题：1）单层访问改多层，封装multiGetSharding? 2) 需要解析key。如果需要解析key，那multiGetSharding还有存在的价值吗？
// 分两步：1）在multi get中，解析多个cmd/key 以及对应的response，然后多层穿透访问；
//        2）将解析req迁移到pipelineToPingPong位置,同时改造req buf。
// TODO：下一步改造：1）支持根据keys来自定义访问指令；2）getmulit在layer层按key hash。

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::backend::AddressEnable;
use crate::{AsyncReadAll, AsyncWriteAll, Response};
use protocol::{Protocol, Request};

use futures::ready;

const REQUEST_BUFF_MAX_LEN: usize = 5000;
pub struct AsyncMultiGet<L, P> {
    // 当前从哪个layer开始发送请求
    idx: usize,
    layers: Vec<L>,
    request_ref: Request,
    //TODO 这里需要改为Option fishermen
    request_rebuild_buf: Vec<u8>,
    response: Option<Response>,
    parser: P,
}

impl<L, P> AsyncMultiGet<L, P>
where
    L: AsyncWriteAll + AsyncWriteAll + AddressEnable + Unpin,
    P: Unpin,
{
    pub fn from_layers(layers: Vec<L>, p: P) -> Self {
        Self {
            idx: 0,
            layers,
            request_ref: Default::default(),
            request_rebuild_buf: Vec::new(),
            response: None,
            parser: p,
        }
    }

    // 发送请求，将current cmds发送到所有mc，如果失败，继续向下一层write，注意处理重入问题
    // ready! 会返回Poll，所以这里还是返回Poll了
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let mut idx = self.idx;

        // 当前layer的reader发送请求，直到发送成功
        while idx < self.layers.len() {
            let reader = unsafe { self.layers.get_unchecked_mut(idx) };
            match ready!(Pin::new(reader).poll_write(cx, &self.request_ref)) {
                Ok(_) => return Poll::Ready(Ok(())),
                Err(_e) => {
                    self.idx += 1;
                    idx = self.idx;
                    log::warn!("mget: write req failed e:{:?}", _e);
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
        // 如果buff太大，释放重建
        if self.request_rebuild_buf.len() > REQUEST_BUFF_MAX_LEN {
            self.request_rebuild_buf = Vec::new();
        }
        self.request_rebuild_buf.clear();
    }
}

impl<L, P> AsyncWriteAll for AsyncMultiGet<L, P>
where
    L: AsyncWriteAll + AsyncWriteAll + AddressEnable + Unpin,
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
    L: AsyncReadAll + AsyncWriteAll + AddressEnable + Unpin,
    P: Unpin + Protocol,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.layers.len());
        let mut last_err = None;

        while me.idx < me.layers.len() {
            let mut found_keys = Vec::new();
            let layer = unsafe { me.layers.get_unchecked_mut(me.idx) };
            let servers = layer.get_address();
            match ready!(Pin::new(layer).poll_next(cx)) {
                Ok(item) => {
                    // 轮询出已经查到的keys
                    found_keys = me.parser.keys_response(item.iter());
                    // TODO 一致性分析需要，在这里打印key及servers
                    if found_keys.len() > 0 {
                        log::info!("gets keys:{:?}, servers: {}", found_keys, servers);
                    }

                    match me.response.as_mut() {
                        Some(response) => response.append(item),
                        None => me.response = Some(item),
                    }
                }
                Err(e) => {
                    log::warn!("get-multi found err: {:?}", e);
                    last_err = Some(e);
                }
            }

            me.idx += 1;
            if me.idx >= me.layers.len() {
                break;
            }

            // 重新构建request cmd，再次请求，生命周期考虑，需要外部出入buf来构建新请求指令
            if found_keys.len() > 0 {
                // 每次重建request，先清理buff
                me.request_rebuild_buf.clear();
                me.parser.rebuild_get_multi_request(
                    &me.request_ref,
                    &found_keys,
                    &mut me.request_rebuild_buf,
                );

                // 如果所有请求全部全部命中，新指令长度为0
                if me.request_rebuild_buf.len() == 0 {
                    break;
                }

                me.request_ref = Request::from(
                    me.request_rebuild_buf.as_slice(),
                    me.request_ref.id().clone(),
                );
                // 及时清理
                found_keys.clear();
                log::debug!("rebuild new req: {:?}", me.request_ref.data());
            }

            if let Err(e) = ready!(me.do_write(cx)) {
                log::warn!("found err when resend layer request:{:?}", e);
                last_err = Some(e);
                break;
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
