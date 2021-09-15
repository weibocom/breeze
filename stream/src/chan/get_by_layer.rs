use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use crate::backend::AddressEnable;
use crate::{AsyncReadAll, AsyncWriteAll, Response};
use protocol::{Operation, Protocol, Request};

use futures::ready;

pub struct AsyncLayerGet<L, P> {
    // 当前从哪个layer开始发送请求
    idx: usize,
    layers: Vec<L>,
    // 每一层访问的请求
    request: Request,
    response: Option<Response>,
    // 用于回写的set noreply响应及请求
    requests_writeback: Option<Vec<Request>>,
    idx_layer_writeback: usize,
    idx_request_writeback: usize,

    parser: P,
    since: Instant, // 上一层请求开始的时间
    polling_resp: bool,
    done: bool,
}

impl<L, P> AsyncLayerGet<L, P>
where
    L: AsyncWriteAll + AsyncWriteAll + AddressEnable + Unpin,
    P: Unpin + Protocol,
{
    pub fn from_layers(layers: Vec<L>, p: P) -> Self {
        Self {
            idx: 0,
            layers,
            request: Default::default(),
            response: None,
            requests_writeback: None,
            idx_layer_writeback: 0,
            idx_request_writeback: 0,
            parser: p,
            since: Instant::now(),
            polling_resp: false,
            done: false,
        }
    }
    // 发送请求，将current cmds发送到所有mc，如果失败，继续向下一层write，注意处理重入问题
    // ready! 会返回Poll，所以这里还是返回Poll了
    #[inline]
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        // 当前layer的reader发送请求，直到发送成功
        let mut last_err = None;
        while self.idx < self.layers.len() {
            log::debug!("write to {}-th/{}", self.idx + 1, self.layers.len());
            let reader = unsafe { self.layers.get_unchecked_mut(self.idx) };
            match ready!(Pin::new(reader).poll_write(cx, &self.request)) {
                Ok(_) => return Poll::Ready(Ok(())),
                Err(e) => {
                    self.idx += 1;
                    last_err = Some(e);
                }
            }
        }

        self.idx = 0;
        // write req到所有资源失败，reset并返回err
        Poll::Ready(Err(last_err.unwrap_or(Error::new(
            ErrorKind::NotConnected,
            "layer get do write error",
        ))))
    }

    // 查到response之后，需要进行构建set指令进行回种：
    // 1 先支持getk/getkq这种带key的response；
    // 2 测试完毕后，再统一支持getq这种不带key的response
    #[inline(always)]
    fn on_response(&mut self, item: Response) {
        // 构建回种的请求
        if self.idx > 0 {
            log::info!("ceate request for write back with idx:{}", self.idx);
            self.create_requests_wb(&item);
        }

        let found = item.keys_num();
        // 记录metrics
        let elapse = self.since.elapsed();
        self.since = Instant::now();
        let metric_id = item.rid().metric_id();
        metrics::qps(get_key_hit_name_by_idx(self.idx), found, metric_id);
        metrics::duration(get_name_by_idx(self.idx), elapse, metric_id);

        match self.request.operation() {
            Operation::Gets => {
                match self.response.as_mut() {
                    Some(response) => response.append(item),
                    None => self.response = Some(item),
                };
            }
            _ => {
                self.response = Some(item);
            }
        }
    }

    fn create_requests_wb(&mut self, resp: &Response) {
        debug_assert!(self.requests_writeback.is_none());
        if self.idx == 0 {
            return;
        }

        // TODO 暂定为3天，这个过期时间后续要从vintage中获取
        let expire_seconds = 3 * 24 * 3600;

        // 轮询response构建回写的request buff及keys
        let rsp_its = resp.iter();
        let mut requests_wb: Vec<Request> = Vec::new();
        for rit in rsp_its {
            if rit.keys().len() == 0 {
                continue;
            }
            let req_rs =
                self.parser
                    .convert_to_writeback_request(&self.request, rit, expire_seconds);

            if let Ok(mut reqs) = req_rs {
                requests_wb.append(&mut reqs);
            } else {
                log::warn!(
                    "careful - not found keys from response items/{}, err: {:?}",
                    resp.items.len(),
                    req_rs.err()
                );
            }
        }

        if requests_wb.len() == 0 {
            log::info!("+++++++ not found request for wb");
            return;
        }

        self.requests_writeback = Some(requests_wb);
    }

    // 回种逻辑单独处理，不放在on_response中，否则pending时，需要保留response，逻辑会很ugly
    fn do_write_back(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.requests_writeback.is_none() {
            log::warn!("no requests to write back!");
            return Poll::Ready(Ok(()));
        }
        // 从第0层开始，轮询回写所有回种请求
        if let Some(reqs_wb) = self.requests_writeback.as_mut() {
            while self.idx_layer_writeback < self.idx {
                // 每一层轮询回种所有请求
                while self.idx_request_writeback < reqs_wb.len() {
                    let reader = self.layers.get_mut(self.idx_layer_writeback).unwrap();
                    let addr = reader.get_address();
                    let req = reqs_wb.get_mut(self.idx_request_writeback).unwrap();
                    match ready!(Pin::new(reader).poll_write(cx, req)) {
                        Ok(_) => {
                            self.idx_request_writeback += 1;
                            continue;
                        }
                        Err(e) => {
                            self.idx_request_writeback += 1;
                            log::warn!("write back to layer/{} failed, err: {}", addr, e);
                            continue;
                        }
                    }
                }
                self.idx_layer_writeback += 1;
            }
        }

        // 发送完毕，take走
        self.requests_writeback.take();

        // 回写完毕，回写idx清零
        self.idx_layer_writeback = 0;
        self.idx_request_writeback = 0;
        return Poll::Ready(Ok(()));
    }
}

impl<L, P> AsyncWriteAll for AsyncLayerGet<L, P>
where
    L: AsyncWriteAll + AsyncWriteAll + AddressEnable + Unpin,
    P: Unpin + Protocol,
{
    // 请求某一层
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, req: &Request) -> Poll<Result<()>> {
        if self.request.len() == 0 {
            self.request = req.clone();
            self.since = Instant::now();
        }
        return self.do_write(cx);
    }
}

impl<L, P> AsyncReadAll for AsyncLayerGet<L, P>
where
    L: AsyncReadAll + AsyncWriteAll + AddressEnable + Unpin,
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.layers.len());
        let mut last_err = None;

        // 回写没有完成，先完成回种
        if !me.requests_writeback.is_none() {
            match ready!(me.do_write_back(cx)) {
                _ => {}
            }
            // 回写完毕，可能需要继续处理下一个layer
            if me.idx + 1 < me.layers.len() {
                me.idx += 1;
            }
        }

        // 如果idx不为0，说明当前是pending重入，先写完request
        if !me.done && !me.polling_resp && me.idx != 0 {
            if let Err(e) = ready!(me.do_write(cx)) {
                log::warn!("found err when resend layer request:{:?}", e);
                last_err = Some(e);
                me.done = true;
            }
        }

        while me.idx < me.layers.len() {
            let layer = unsafe { me.layers.get_unchecked_mut(me.idx) };
            //let _servers = layer.get_address();
            me.polling_resp = true;
            match ready!(Pin::new(layer).poll_next(cx)) {
                Ok(item) => {
                    me.polling_resp = false;
                    // 轮询出已经查到的keys
                    match me.parser.filter_by_key(&me.request, item.iter()) {
                        None => {
                            // 所有请求都已返回
                            // 所有请求都已返回
                            me.done = true;
                            me.on_response(item);
                            match ready!(me.do_write_back(cx)) {
                                Err(e) => log::warn!("found err when write back: {:?}", e),
                                _ => {}
                            }
                            break;
                        }
                        Some(req) => {
                            me.request = req;
                            me.on_response(item);

                            match ready!(me.do_write_back(cx)) {
                                Err(e) => log::warn!("found err when write back: {:?}", e),
                                _ => {}
                            }
                        }
                    }
                }
                Err(e) => {
                    log::debug!("found err: {:?} idx:{}", e, me.idx);
                    last_err = Some(e);
                }
            }

            me.idx += 1;
            if me.idx >= me.layers.len() {
                break;
            }

            if let Err(e) = ready!(me.do_write(cx)) {
                log::warn!("found err when resend layer request:{:?}", e);
                last_err = Some(e);
                break;
            }
        }

        // 先拿走response，然后重置，最后返回响应列表
        me.idx = 0;
        let response = me.response.take();
        let old = std::mem::take(&mut self.request);
        drop(old);
        // 请求完毕，重置
        response
            .map(|item| Poll::Ready(Ok(item)))
            .unwrap_or_else(|| {
                Poll::Ready(Err(last_err.unwrap_or_else(|| {
                    Error::new(
                        ErrorKind::Other,
                        "not error found, may be. layers not configured properly",
                    )
                })))
            })
    }
}

const NAMES: &[&'static str] = &["l0", "l1", "l2", "l3", "l4", "l5", "l6", "l7"];
fn get_name_by_idx(idx: usize) -> &'static str {
    if idx >= NAMES.len() {
        "hit_lunkown"
    } else {
        unsafe { NAMES.get_unchecked(idx) }
    }
}
const NAMES_HIT: &[&'static str] = &[
    "l0_hit_key",
    "l1_hit_key",
    "l2_hit_key",
    "l3_hit_key",
    "l4_hit_key",
    "l5_hit_key",
    "l6_hit_key",
    "l7_hit_key",
];
fn get_key_hit_name_by_idx(idx: usize) -> &'static str {
    if idx >= NAMES_HIT.len() {
        "hit_lunkown"
    } else {
        unsafe { NAMES_HIT.get_unchecked(idx) }
    }
}
