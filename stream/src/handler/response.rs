use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use ds::ResizedRingBuffer;
use metrics::MetricName;
use protocol::Protocol;

use futures::ready;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::{Instant, interval, Interval};

pub trait Handler {
    // 获取自上一次调用以来，成功读取并可以释放的字节数量
    fn load_read(&self) -> usize;
    // 从backend接收到response，并且完成协议解析时调用
    fn on_received(&self, seq: usize, response: protocol::Response);
    fn running(&self) -> bool;
}

unsafe impl<R, W, P> Send for ResponseHandler<R, W, P> {}
unsafe impl<R, W, P> Sync for ResponseHandler<R, W, P> {}

pub struct ResponseHandler<R, W, P> {
    seq: usize,
    r: R,
    w: W,
    parser: P,
    data: ResizedRingBuffer,

    metric_id: usize,
    tick: Interval,
    ticks: usize,

    processed: usize,
}

impl<R, W, P> ResponseHandler<R, W, P> {
    pub fn from(r: R, w: W, parser: P, mid: usize) -> Self
    where
        W: Handler + Unpin,
    {
        let data = ResizedRingBuffer::new(move |old, delta| {
            if delta > old as isize && delta >= 32 * 1024 {
                // 扩容的时候才输出日志
                log::info!("buffer resized ({}, {}). {}", old, delta, mid.name());
            }
            metrics::count("mem_buff_resp", delta, mid);
        });

        Self {
            seq: 0,
            w: w,
            r: r,
            parser: parser,
            data: data,
            ticks: 0,
            tick: interval(Duration::from_micros(500)),
            metric_id: mid,
            processed: 0,
        }
    }
}

impl<R, W, P> Future for ResponseHandler<R, W, P>
where
    R: AsyncRead + Unpin,
    P: Protocol + Unpin,
    W: Handler + Unpin,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        log::info!("come into response handler poll");
        let me = &mut *self;
        let mut reader = Pin::new(&mut me.r);
        let mut eof = false;
        while me.w.running() {
            log::info!("response handler poll loop");
            let read = me.w.load_read();
            me.data.advance_read(read);
            let mut buf = me.data.as_mut_bytes();
            if buf.len() == 0 {
                log::info!("come into response handler poll_tick");
                ready!(me.tick.poll_tick(cx));
                continue;
            }
            me.ticks = 0;
            let mut buf = ReadBuf::new(&mut buf);
            log::info!("come into response handler poll_read");
            ready!(reader.as_mut().poll_read(cx, &mut buf))?;
            let n = buf.capacity() - buf.remaining();
            if n == 0 {
                eof = true;
                continue; // EOF
            }
            me.data.advance_write(n);
            let p_oft = me.processed - me.data.read();
            let processing = me.data.data().sub_slice(p_oft, me.data.len() - p_oft);
            let mut data_string = String::from_utf8(processing.data()).unwrap();
            data_string = data_string.replace("\r", "\\r");
            data_string = data_string.replace("\n", "\\n");
            log::info!("received from redis, data = {}", data_string);

            // 处理等处理的数据
            while me.processed < me.data.writtened() {
                let p_oft = me.processed - me.data.read();
                let processing = me.data.data().sub_slice(p_oft, me.data.len() - p_oft);
                let parse_begin = Instant::now();
                match me.parser.parse_response(&processing) {
                    None => break,
                    Some(r) => {
                        let parse_end = Instant::now();
                        let seq = me.seq;
                        me.seq += 1;
                        me.processed += r.len();
                        me.w.on_received(seq, r);
                        let on_recv_end = Instant::now();
                        let parse_content = String::from_utf8(processing.data()).unwrap().replace("\r", "\\r").replace("\n", "\\n");
                        log::info!("parse {} cost {:?}, on recv cost {:?}", parse_content, parse_end.duration_since(parse_begin), on_recv_end.duration_since(parse_end));
                        //metrics::ratio("mem_buff_resp", me.data.ratio(), me.metric_id);
                    }
                }
            }
        }
        log::info!("task complete:eof = {}, running = {}, me = {} ", eof, me.w.running(),me);
        Poll::Ready(Ok(()))
    }
}
use std::fmt::{self, Display, Formatter};
impl<R, W, P> Display for ResponseHandler<R, W, P> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} - seq:{} buffer:{} processed:{:?}",
            self.metric_id.name(),
            self.seq,
            self.data,
            self.processed
        )
    }
}
