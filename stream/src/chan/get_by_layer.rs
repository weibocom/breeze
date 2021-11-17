use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use crate::{Address, Addressed, AsyncReadAll, AsyncWriteAll, LayerRoleAble, Response};
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
    master_idx: usize,
    is_gets: bool,
    parser: P,
    since: Instant, // 上一层请求开始的时间
    err: Option<Error>,
    do_writeback: bool,
}

impl<L, B, P> AsyncLayerGet<L, B, P>
where
    L: AsyncWriteAll + AsyncWriteAll + Addressed + LayerRoleAble + Unpin,
    B: AsyncWriteAll + AsyncWriteAll + Addressed + Unpin,
    P: Unpin + Protocol,
{
    pub fn from_layers(layers: Vec<L>, layers_writeback: Vec<B>, p: P) -> Self {
        let mut do_writeback = false;
        if layers_writeback.len() > 0 {
            assert_eq!(layers.len(), layers_writeback.len());
            do_writeback = true;
        }
        let mut master_idx = 0;
        for layer in layers.iter() {
            if layer.is_master() {
                break;
            }
            master_idx += 1;
        }
        if master_idx >= layers.len() {
            master_idx = 0;
            log::error!("not found master idx: {:?}", layers.addr());
        }
        Self {
            idx: 0,
            layers,
            layers_writeback,
            request: Default::default(),
            response: None,
            master_idx,
            is_gets: false,
            parser: p,
            since: Instant::now(),
            err: None,
            do_writeback,
        }
    }
    // get: 遍历所有层次，发送请求，直到一个成功。有一层成功返回true，更新层次索引，否则返回false
    // gets: 只请求master
    #[inline]
    fn do_write(&mut self, cx: &mut Context<'_>) -> Poll<bool> {
        // 目前只有gets，还有其他的，再考虑更统一的方案 fishermen
        // 对于gets，需要特殊处理：只请求master
        if !self.is_gets {
            self.is_gets = self.parser.req_gets(&self.request);
            if self.is_gets {
                self.idx = self.master_idx;
                self.parser.convert_gets(&self.request);
            }
        }

        while self.idx < self.layers.len() {
            log::debug!(
                "write to {}-th/{}:{:?}",
                self.idx + 1,
                self.layers.len(),
                self.request.data()
            );
            let reader = unsafe { self.layers.get_unchecked_mut(self.idx) };
            match ready!(Pin::new(reader).poll_write(cx, &self.request)) {
                Ok(_) => return Poll::Ready(true),
                Err(e) => {
                    self.idx += 1;
                    self.err = Some(e);
                }
            }
            // 对于gets,只请求一次，且只请求master，到了这里说明，请求失败
            if self.is_gets {
                break;
            }
        }

        Poll::Ready(false)
    }

    // 只在打开Debug时打印并开启
    #[inline(always)]
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

        let found = item.keys_num();

        // 构建回种的cmd，并进行回种操作，注意对gets不进行回种
        if self.do_writeback && self.idx > 0 && found > 0 && !self.is_gets {
            self.do_write_back(cx, &item);
        }

        // 记录metrics
        let elapse = self.since.elapsed();
        self.since = Instant::now();
        let metric_id = item.rid().metric_id();
        metrics::qps(get_key_hit_name_by_idx(self.idx), found, metric_id);
        metrics::duration(get_name_by_idx(self.idx), elapse, metric_id);

        match self.request.operation() {
            Operation::MGet => {
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

    // precondition：私有方法，调用者需要确保: self.idx>0
    #[inline]
    fn do_write_back(&mut self, cx: &mut Context<'_>, resp: &Response) {
        debug_assert!(self.idx > 0);
        // TODO 暂定为1天，这个过期时间后续要从vintage中获取
        const EXP_SEC: u32 = 24 * 3600;

        // 轮询response构建回写的request buff及keys
        let mut keys = 0; // 回程的key
        for rit in resp.iter() {
            if rit.keys().len() == 0 {
                continue;
            }
            if let Ok(reqs) = self
                .parser
                .convert_to_writeback_request(&self.request, rit, EXP_SEC)
            {
                keys += reqs.len(); // 一个回种的请求只有一个key
                for req in reqs {
                    debug_assert_eq!(req.keys().len(), 1);
                    debug_assert!(req.noreply());
                    // 只回程前n-1层
                    for i in 0..self.idx {
                        let reader = self.layers_writeback.get_mut(i).unwrap();
                        let _ = Pin::new(reader).poll_write(cx, &req);
                    }
                }
            }
        }
        metrics::qps("back_key", keys, resp.rid().metric_id());
    }
}

impl<L, B, P> AsyncWriteAll for AsyncLayerGet<L, B, P>
where
    L: AsyncWriteAll + AsyncWriteAll + Addressed + LayerRoleAble + Unpin,
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
        if ready!(self.do_write(cx)) {
            Poll::Ready(Ok(()))
        } else {
            // 请求失败，reset
            self.idx = 0;
            self.is_gets = false;
            self.response.take();
            let old = std::mem::take(&mut self.request);
            drop(old);
            let last_err = self.err.take();

            Poll::Ready(Err(last_err.unwrap_or(Error::new(
                ErrorKind::NotConnected,
                format!("layer poll write error. layers:{}", self.layers.len()),
            ))))
        }
    }
}

impl<L, B, P> AsyncReadAll for AsyncLayerGet<L, B, P>
where
    L: AsyncReadAll + AsyncWriteAll + Addressed + LayerRoleAble + Unpin,
    B: AsyncReadAll + AsyncWriteAll + Addressed + Unpin,
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.layers.len());
        loop {
            let layer = unsafe { me.layers.get_unchecked_mut(me.idx) };
            match ready!(Pin::new(layer).poll_next(cx)) {
                Ok(item) => {
                    // 轮询出已经查到的keys
                    match me.parser.filter_by_key(&me.request, item.iter()) {
                        None => {
                            me.on_response(cx, item);
                            break;
                        }
                        Some(req) => {
                            me.on_response(cx, item);
                            me.request = req;
                        }
                    }
                }
                Err(e) => {
                    log::debug!("found err: {:?} idx:{}", e, me.idx);
                    me.err = Some(e);
                }
            }
            if me.is_gets {
                break;
            }

            me.idx += 1;
            if !ready!(me.do_write(cx)) {
                break;
            }
        }

        // 先拿走response，然后重置，最后返回响应列表
        me.idx = 0;
        me.is_gets = false;
        let response = me.response.take();
        let old = std::mem::take(&mut me.request);
        drop(old);

        // 请求完毕，重置
        let last_err = me.err.take();
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
#[inline(always)]
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
#[inline(always)]
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
