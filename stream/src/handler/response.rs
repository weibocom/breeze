use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use ds::ResizedRingBuffer;
use metrics::MetricName;
use protocol::Protocol;

use futures::ready;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::{interval, Interval};

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

    metric_id: usize,
    tick: Interval,
    ticks: usize,
}

impl<R, W, P> BridgeResponseToLocal<R, W, P> {
    pub fn from(r: R, w: W, parser: P, done: Arc<AtomicBool>, mid: usize) -> Self
    where
        W: ResponseHandler + Unpin,
    {
        let mut data = ResizedRingBuffer::new();
        metrics::count("mem_buff_resp", data.cap() as isize, mid);
        data.set_on_resize(move |old, delta| {
            log::info!("buffer resized ({}, {}). {}", old, delta, mid.name());
            metrics::count("mem_buff_resp", delta, mid);
        });

        Self {
            seq: 0,
            w: w,
            r: r,
            parser: parser,
            done: done,
            data: data,
            ticks: 0,
            tick: interval(Duration::from_micros(500)),
            metric_id: mid,
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
            if buf.len() == 0 {
                ready!(me.tick.poll_tick(cx));
                continue;
            }
            me.ticks = 0;
            let mut buf = ReadBuf::new(&mut buf);
            ready!(reader.as_mut().poll_read(cx, &mut buf))?;
            let n = buf.capacity() - buf.remaining();
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

                        metrics::ratio("mem_buff_resp", me.data.ratio(), me.metric_id);

                        me.data.advance_processed(r.len());
                        me.w.on_received(seq, r);
                    }
                }
            }
        }
        log::info!("task complete:{} ", me);
        Poll::Ready(Ok(()))
    }
}
use std::fmt::{self, Display, Formatter};
impl<R, W, P> Display for BridgeResponseToLocal<R, W, P> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} - seq:{} buffer:{} processing:{:?}",
            self.metric_id.name(),
            self.seq,
            self.data,
            self.data.processing_bytes().data()
        )
    }
}
