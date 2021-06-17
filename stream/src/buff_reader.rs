use std::future::Future;
use std::io::{Error, Result};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use super::ResponseRingBuffer;
use super::RingSlice;

use protocol::ResponseParser;

use tokio::io::{AsyncRead, ReadBuf};

use futures::ready;

pub trait Response {
    fn load_read_offset(&self) -> usize;
    fn on_response(&self, seq: usize, response: RingSlice);
}

unsafe impl<R, W, P> Send for BridgeResponseToLocal<R, W, P> {}
unsafe impl<R, W, P> Sync for BridgeResponseToLocal<R, W, P> {}

pub struct BridgeResponseToLocal<R, W, P> {
    seq: usize,
    done: Arc<AtomicBool>,
    r: R,
    w: W,
    parser: P,
    data: ResponseRingBuffer,
}

impl<R, W, P> BridgeResponseToLocal<R, W, P> {
    pub fn from(r: R, w: W, parser: P, buf: usize, done: Arc<AtomicBool>) -> Self {
        debug_assert!(buf == buf.next_power_of_two());
        Self {
            seq: 0,
            w: w,
            r: r,
            parser: parser,
            data: ResponseRingBuffer::with_capacity(buf),
            done: done,
        }
    }
}

impl<R, W, P> Future for BridgeResponseToLocal<R, W, P>
where
    R: AsyncRead + Unpin,
    P: ResponseParser + Unpin,
    W: Response + Unpin,
{
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        println!("task polling. BridgeResponseToLocal");
        let me = &mut *self;
        let mut reader = Pin::new(&mut me.r);
        //let mut spins = 0;
        while !me.done.load(Ordering::Relaxed) {
            let offset = me.w.load_read_offset();
            me.data.reset_read(offset);
            let mut buf = me.data.as_mut_bytes();
            println!(
                "task response to buffer:{} bytes available offset:{}",
                buf.len(),
                offset
            );
            if buf.len() == 0 {
                panic!("response buffer full");
                //println!("response buffer full");
                std::hint::spin_loop();
                continue;
            }
            let mut buf = ReadBuf::new(&mut buf);
            ready!(reader.as_mut().poll_read(cx, &mut buf))?;
            // 一共读取了n个字节
            let n = buf.capacity() - buf.remaining();
            println!(
                "task response to buffer:{} bytes read from response. read buffer filled:{:?}",
                n,
                buf.filled()
            );
            if n == 0 {
                // EOF
                //panic!("EOF FOUND");
                //std::hint::spin_loop();
                break;
            }
            me.data.advance_write(n);
            // 处理等处理的数据
            while me.data.processed() < me.data.writtened() {
                let mut response = me.data.processing_bytes();
                let (found, num) = me.parser.parse_response(&response);
                println!(
                    "task response to buffer: response processing bytes:{} parsed:{} num:{} seq:{} processed:{} written:{}",
                    response.available(),
                    found,
                    num,
                    me.seq,
                    me.data.processed(),
                    me.data.writtened(),
                );
                if !found {
                    if num >= 24 {
                        panic!("not a valid response");
                    }
                    break;
                }
                response.resize(num);
                let seq = me.seq;
                me.w.on_response(seq, response);
                println!(
                    "task response to buffer:  {} bytes processed. seq:{} {} => {}",
                    num,
                    seq,
                    me.data.processed(),
                    me.data.writtened(),
                );
                me.data.advance_processed(num);
                me.seq += 1;
            }
        }
        println!("task of reading data from response complete");
        Poll::Ready(Ok(()))
    }
}
