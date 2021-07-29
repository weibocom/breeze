use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use ds::{ResizedRingBuffer, RingSlice};

use protocol::Protocol;

use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::{interval, Interval};

use futures::ready;

pub trait ResponseHandler {
    fn load_offset(&self) -> usize;
    // 从backend接收到response，并且完成协议解析时调用
    fn on_received(&self, seq: usize, response: RingSlice);
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
}

impl<R, W, P> BridgeResponseToLocal<R, W, P> {
    pub fn from(r: R, w: W, parser: P, done: Arc<AtomicBool>) -> Self {
        Self {
            seq: 0,
            w: w,
            r: r,
            parser: parser,
            data: ResizedRingBuffer::new(),
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
        log::debug!("resp-handler: task polling.");
        let me = &mut *self;
        let mut reader = Pin::new(&mut me.r);
        while !me.done.load(Ordering::Acquire) {
            let offset = me.w.load_offset();
            me.data.reset_read(offset);
            let mut buf = me.data.as_mut_bytes();
            log::debug!("resp-handler: {} bytes available oft:{}", buf.len(), offset);
            if buf.len() == 0 {
                if me.spins == 0 {
                    me.spin_last = Instant::now();
                }
                me.spins += 1;
                std::hint::spin_loop();
                if me.spins >= 1024 {
                    // 进行resize
                    if me.data.resize() {
                        continue;
                    }
                    // 进行sleep 1ms
                    ready!(me.tick.poll_tick(cx));
                }
                // 每超过一秒钟输出一次日志
                let secs = me.spin_last.elapsed().as_secs() as usize;
                if secs > me.spin_secs {
                    log::info!(
                        "resp-handler: buffer full. read:{} processed:{} write:{}",
                        offset,
                        me.data.processed(),
                        me.data.writtened()
                    );
                    me.spin_secs = secs;
                }
                continue;
            }
            me.spins = 0;
            let mut buf = ReadBuf::new(&mut buf);
            ready!(reader.as_mut().poll_read(cx, &mut buf))?;
            // 一共读取了n个字节
            let n = buf.capacity() - buf.remaining();
            log::debug!(
                "resp-handler:{} bytes read. data:{:?}",
                n,
                &buf.filled()[0..n.min(48)]
            );
            if n == 0 {
                break; // EOF
            }
            me.data.advance_write(n);
            // 处理等处理的数据
            while me.data.processed() < me.data.writtened() {
                let mut response = me.data.processing_bytes();
                let (found, num) = me.parser.parse_response(&response);
                log::debug!("resp-handler: parsed:{} num:{} seq:{} ", found, num, me.seq);
                if !found {
                    break;
                }
                response.resize(num);
                let seq = me.seq;
                me.seq += 1;

                me.w.on_received(seq, response);
                me.data.advance_processed(num);
            }
        }
        log::info!("resp-handler: task of reading data from response complete");
        Poll::Ready(Ok(()))
    }
}
