use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use tokio::io::{AsyncWrite, BufWriter};

use metrics::MetricName;
use protocol::Request;
use crate::Addressed;

pub struct Snapshot {
    cids: Vec<usize>,
    reqs: Vec<Request>,
}
impl Snapshot {
    fn new() -> Self {
        Self {
            cids: Vec::with_capacity(64),
            reqs: Vec::with_capacity(8),
        }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.cids.len() + self.reqs.len()
    }
    #[inline(always)]
    pub fn push_all(&mut self, cids: Vec<usize>) {
        let _ = std::mem::replace(&mut self.cids, cids);
    }
    #[inline(always)]
    pub fn push_one(&mut self, req: Request) {
        self.reqs.push(req);
    }
    // 调用方确保idx < cids.len()
    #[inline(always)]
    fn take<H>(&mut self, handler: &H, seq: usize) -> Option<(usize, Request)>
    where
        H: Unpin + crate::handler::request::Handler,
    {
        match self.cids.pop() {
            Some(cid) => handler.take(cid, seq),
            None => self.reqs.pop().map(|req| (std::usize::MAX, req)),
        }
    }
}
pub trait Handler {
    // 如果填充ss的长度为0，则说明handler没有要处理的数据流，提示到达eof。
    fn poll_fill_snapshot(&self, cx: &mut Context, ss: &mut Snapshot) -> Poll<()>;
    fn take(&self, cid: usize, seq: usize) -> Option<(usize, Request)>;
    fn sent(&self, cid: usize, seq: usize, req: &Request);
    fn running(&self) -> bool;
}

pub struct RequestHandler<H, W> {
    snapshot: Snapshot,
    // 当前处理的请求
    cache: Option<(usize, Request)>,
    // 当前请求的data已经写入到writer的字节数量
    offset: usize,
    seq: usize,
    handler: H,
    w: BufWriter<W>,
    metric_id: usize,
}

const WRITE_BUFF: usize = 8 * 1024;
impl<H, W> RequestHandler<H, W> {
    pub fn from(handler: H, w: W, mid: usize) -> Self
    where
        W: AsyncWrite,
    {
        // 在Drop时，会减掉
        metrics::count("mem_buff_req", WRITE_BUFF as isize, mid);
        Self {
            metric_id: mid,
            seq: 0,
            handler: handler,
            w: BufWriter::with_capacity(WRITE_BUFF, w),
            snapshot: Snapshot::new(),
            cache: None,
            offset: 0,
        }
    }
}
impl<H, W> Future for RequestHandler<H, W>
where
    H: Unpin + Handler + Addressed,
    W: AsyncWrite + Unpin,
{
    type Output = Result<()>;

    #[inline(always)]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        let mut w = Pin::new(&mut me.w);
        while me.handler.running() {
            if let Some((ref cid, ref req)) = me.cache {
                let data = req.data();
                while me.offset < data.len() {
                    let n = ready!(w.as_mut().poll_write(cx, &data[me.offset..]))?;
                    //let data_str = String::from_utf8(data[me.offset..].to_vec());
                    //if data_str.is_ok() {
                    //    log::info!("seq: {}, send to redis: {:?} data: {}", me.seq, me.handler.addr(), data_str.unwrap().replace("\r\n", "\\r\\n"));
                    //}
                    me.offset += n;
                }

                me.handler.sent(*cid, me.seq, req);

                // 如果是noreply，则序号不需要增加。因为没有response
                if !req.noreply() {
                    me.seq += 1;
                }
            }
            me.offset = 0;
            if let Some((cid, req)) = me.snapshot.take(&me.handler, me.seq) {
                me.cache = Some((cid, req));
                continue;
            }
            me.cache.take();
            match me.handler.poll_fill_snapshot(cx, &mut me.snapshot) {
                Poll::Ready(_) => {
                    if me.snapshot.len() == 0 {
                        break;
                    }
                }
                Poll::Pending => {
                    log::debug!("pending. seq:{}", me.seq);
                    ready!(w.as_mut().poll_flush(cx))?;
                    return Poll::Pending;
                }
            }
        }

        ready!(w.as_mut().poll_shutdown(cx))?;
        log::info!("task complete:{} seq:{}", me.metric_id.name(), me.seq);
        Poll::Ready(Ok(()))
    }
}
impl<H, W> Drop for RequestHandler<H, W> {
    #[inline]
    fn drop(&mut self) {
        metrics::count("mem_buff_req", WRITE_BUFF as isize * -1, self.metric_id);
    }
}
