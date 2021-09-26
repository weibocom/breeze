use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use crate::{Address, Addressed, AsyncReadAll, AsyncWriteAll, Response};
use protocol::{Operation, Protocol, Request};

use futures::ready;

pub struct AsyncLayerGet<L, B, P> {
    // 当前从哪个layer开始发送请求
    idx: usize,
    layers: Vec<L>,
    layers_writeback: Vec<B>,
    // 每一层访问的请求
    request: Request,
    response: Option<Response>,
    // 用于回写的set noreply响应及请求
    requests_writeback: Option<Vec<Request>>,
    idx_layer_writeback: usize,
    parser: P,
    since: Instant, // 上一层请求开始的时间
}

impl<L, B, P> AsyncLayerGet<L, B, P>
where
    L: AsyncWriteAll + AsyncWriteAll + Addressed + Unpin,
    B: AsyncWriteAll + AsyncWriteAll + Addressed + Unpin,
    P: Unpin + Protocol,
{
    pub fn from_layers(layers: Vec<L>, layers_writeback: Vec<B>, p: P) -> Self {
        Self {
            idx: 0,
            layers,
            layers_writeback,
            request: Default::default(),
            response: None,
            requests_writeback: None,
            idx_layer_writeback: 0,
            parser: p,
            since: Instant::now(),
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

    // 只在打开Debug时打印并开启
    fn log_response(&mut self, item: &Response) {
        if !log::log_enabled!(log::Level::Debug) {
            return;
        }
        // print request and respons
        log::debug!("=================== print response... =================");
        let rsp_its = item.iter();
        for rit in rsp_its {
            let mut data = Vec::with_capacity(rit.len());
            rit.as_ref().copy_to_vec(&mut data);
            log::debug!("resp data: {:?}", data);
        }
        log::debug!("=================== printed response!!! =================");
    }

    // 查到response之后，需要进行构建set指令进行回种：
    // 1 先支持getk/getkq这种带key的response；
    // 2 测试完毕后，再统一支持getq这种不带key的response
    #[inline(always)]
    fn on_response(&mut self, cx: &mut Context<'_>, item: Response) {
        // 暂时保留，查问题的时候和sharding的req日志结合排查
        self.log_response(&item);

        // 构建回种的cmd，并进行回种操作
        if self.idx > 0 {
            self.create_requests_wb(&item);
            self.do_write_back(cx);
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
            log::info!("should not create request for write back when call layer-0");
            return;
        }

        // TODO 暂定为1天，这个过期时间后续要从vintage中获取
        let expire_seconds = 24 * 3600;

        // 轮询response构建回写的request buff及keys
        let rsp_its = resp.iter();
        let mut requests_wb: Vec<Request> = Vec::new();
        for rit in rsp_its {
            if rit.keys().len() == 0 {
                if log::log_enabled!(log::Level::Debug) {
                    let mut data = Vec::with_capacity(rit.len());
                    rit.as_ref().copy_to_vec(&mut data);
                    log::debug!("found empty keys response, data: {:?}", data);
                }
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
            return;
        }
        //记录回种的kps，metric
        metrics::qps("back_key", requests_wb.len(), resp.rid().metric_id());

        self.requests_writeback = Some(requests_wb);
    }

    // 回种逻辑单独处理，不放在on_response中，否则处理pending时，需要保留response，逻辑会很ugly
    fn do_write_back(&mut self, cx: &mut Context<'_>) {
        if self.requests_writeback.is_none() {
            return;
        }

        // 从第0层开始，轮询回写所有回种请求
        if let Some(reqs_wb) = self.requests_writeback.as_mut() {
            while self.idx_layer_writeback < self.idx {
                // 每一层轮询回种所有请求
                let mut i = 0;
                while i < reqs_wb.len() {
                    let reader = self
                        .layers_writeback
                        .get_mut(self.idx_layer_writeback)
                        .unwrap();
                    let addr = reader.addr();
                    let req = reqs_wb.get_mut(i).unwrap();
                    log::debug!(
                        "layer/{}/{} will write back req: {:?} to sever : {:?}",
                        self.idx_layer_writeback,
                        self.idx,
                        req.data(),
                        addr
                    );
                    let _ = Pin::new(reader).poll_write(cx, req);
                    i += 1;
                }
                self.idx_layer_writeback += 1;
            }
        }

        // 发送完毕，take走
        self.requests_writeback.take();

        // 回写完毕，回写idx清零
        self.idx_layer_writeback = 0;
        // return Poll::Ready(Ok(()));
    }
}

impl<L, B, P> AsyncWriteAll for AsyncLayerGet<L, B, P>
where
    L: AsyncWriteAll + AsyncWriteAll + Addressed + Unpin,
    B: AsyncWriteAll + AsyncWriteAll + Addressed + Unpin,
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

impl<L, B, P> AsyncReadAll for AsyncLayerGet<L, B, P>
where
    L: AsyncReadAll + AsyncWriteAll + Addressed + Unpin,
    B: AsyncReadAll + AsyncWriteAll + Addressed + Unpin,
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.layers.len());
        let mut last_err = None;

        while me.idx < me.layers.len() {
            let layer = unsafe { me.layers.get_unchecked_mut(me.idx) };
            //let _servers = layer.get_address();
            match ready!(Pin::new(layer).poll_next(cx)) {
                Ok(item) => {
                    // 轮询出已经查到的keys
                    match me.parser.filter_by_key(&me.request, item.iter()) {
                        None => {
                            // 处理response，并会根据req进行回写操作
                            me.on_response(cx, item);
                            break;
                        }
                        Some(req) => {
                            // 处理response，并会根据req构建回写请求并进行回写操作
                            me.on_response(cx, item);
                            me.request = req;
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

impl<L, B, P> Addressed for AsyncLayerGet<L, B, P>
where
    L: Addressed,
{
    fn addr(&self) -> Address {
        self.layers.addr()
    }
}
