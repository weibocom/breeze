use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ds::ResizedRingBuffer;
use metrics::MetricName;
use protocol::Protocol;

use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::{interval, Interval};

use futures::ready;

pub trait ResponseHandler {
    fn load_offset(&self) -> usize;
    // 从backend接收到response，并且完成协议解析时调用
    fn on_received(&self, seq: usize, response: protocol::Response);
}

unsafe impl<R, W, P> Send for BridgeResponseToLocal<R, W, P> {}
unsafe impl<R, W, P> Sync for BridgeResponseToLocal<R, W, P> {}

pub struct BridgeResponseToLocal<R, W, P> {
    seq: usize,
    done: Arc<AtomicBool>,
    r: R,
    w: W,
    parser: P,
    data: ResizedRingBuffer,

    // 用来做spin控制，避免长时间spin
    spins: usize,
    spin_secs: usize,   // spin已经持续的时间，用来控制输出日志
    spin_last: Instant, // 上一次开始spin的时间
    tick: Interval,
    metric_id: usize,
}

impl<R, W, P> BridgeResponseToLocal<R, W, P> {
    pub fn from(r: R, w: W, parser: P, done: Arc<AtomicBool>, mid: usize) -> Self
    where
        W: ResponseHandler + Unpin,
    {
        let cap = 64 * 1024;
        metrics::count("mem_buff_resp", cap, mid);
        Self {
            metric_id: mid,
            seq: 0,
            w: w,
            r: r,
            parser: parser,
            data: ResizedRingBuffer::with_capacity(cap as usize),
            done: done,

            spins: 0,
            spin_last: Instant::now(),
            tick: interval(Duration::from_micros(1)),
            spin_secs: 0,
        }
    }
}

impl<R, W, P> Future for BridgeResponseToLocal<R, W, P>
where
    R: AsyncRead + Unpin,
    P: Protocol + Unpin,
    W: ResponseHandler + Unpin,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        let mut reader = Pin::new(&mut me.r);
        while !me.done.load(Ordering::Acquire) {
            let offset = me.w.load_offset();
            me.data.reset_read(offset);
            let mut buf = me.data.as_mut_bytes();
            log::debug!("{} bytes available oft:{}", buf.len(), offset);
            if buf.len() == 0 {
                if me.spins == 0 {
                    me.spin_last = Instant::now();
                }
                me.spins += 1;
                std::hint::spin_loop();
                // 1024这个值并没有参考与借鉴。
                if me.spins & 1023 == 0 {
                    ready!(me.tick.poll_tick(cx));
                    continue;
                }
                let last = me.spin_last.elapsed();
                if last >= Duration::from_millis(5) {
                    if me.data.resize() {
                        metrics::count("mem_buff_resp", (me.data.cap() / 2) as isize, me.metric_id);
                        log::info!("{} resized {} {:?}", me.metric_id.name(), me.data, last);
                        continue;
                    }
                }
                // 每超过一秒钟输出一次日志
                let secs = last.as_secs() as usize;
                if secs > me.spin_secs {
                    log::info!("{} full {} -{}", me.metric_id.name(), me.data, me.seq);
                    me.spin_secs = secs;
                }
                continue;
            }
            me.spins = 0;
            let mut buf = ReadBuf::new(&mut buf);
            ready!(reader.as_mut().poll_read(cx, &mut buf))?;
            let n = buf.capacity() - buf.remaining();
            log::debug!("{} bytes read.", n);
            if n == 0 {
                break; // EOF
            }
            me.data.advance_write(n);
            // 处理等处理的数据
            while me.data.processed() < me.data.writtened() {
                let response = me.data.processing_bytes();
                match me.parser.parse_response(&response) {
                    None => break,
                    Some(r) => {
                        let seq = me.seq;
                        me.seq += 1;

                        me.data.advance_processed(r.len());
                        me.w.on_received(seq, r);
                    }
                }
            }
        }
        log::info!("task complete:{}", me.metric_id.name());
        Poll::Ready(Ok(()))
    }
}
impl<R, W, P> Drop for BridgeResponseToLocal<R, W, P> {
    #[inline]
    fn drop(&mut self) {
        metrics::count("mem_buff_resp", self.data.cap() as isize, self.metric_id);
    }
}
