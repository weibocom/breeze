use std::future::Future;
use std::io::{Error, Result};
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

use super::ResponseRingBuffer;
use super::RingSlice;

use protocol::ResponseParser;

use tokio::io::{AsyncRead, ReadBuf};

use futures::ready;

pub trait Response {
    fn load_read_offset(&self) -> usize;
    fn on_full(&self, waker: Waker);
    fn on_response(&self, seq: usize, response: RingSlice);
    fn on_error(&self, err: Error);
}

unsafe impl<'a, T, R, P> Send for BuffReadFrom<'a, T, R, P> {}
unsafe impl<'a, T, R, P> Sync for BuffReadFrom<'a, T, R, P> {}

pub struct BuffReadFrom<'a, T, R, P> {
    seq: usize,
    cond: &'a T,
    r: R,
    parser: P,

    data: ResponseRingBuffer,
}

impl<'a, T, R, P> BuffReadFrom<'a, T, R, P> {
    pub fn from(cond: &'a T, r: R, parser: P, buf: usize) -> Self {
        debug_assert!(buf == buf.next_power_of_two());
        Self {
            seq: 0,
            cond: cond,
            r: r,
            parser: parser,
            data: ResponseRingBuffer::with_capacity(buf),
        }
    }
}

impl<'a, T, R, P> Future for BuffReadFrom<'a, T, R, P>
where
    R: AsyncRead + Unpin,
    P: ResponseParser + Unpin,
    T: Response + Unpin,
{
    type Output = Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = &mut *self;
        let mut reader = Pin::new(&mut me.r);
        loop {
            me.data.reset_read(me.cond.load_read_offset());
            let mut buf = me.data.as_mut_bytes();
            if buf.len() == 0 {
                me.cond.on_full(cx.waker().clone());
                return Poll::Pending;
            }
            let mut buf = ReadBuf::new(&mut buf);
            ready!(reader.as_mut().poll_read(cx, &mut buf))?;
            // 一共读取了n个字节
            let n = buf.capacity() - buf.remaining();
            //println!("{} bytes read from response", n);
            if n == 0 {
                // EOF
                return Poll::Ready(Ok(me.seq));
            }
            me.data.advance_write(n);
            // 处理等处理的数据
            while me.data.processed() < me.data.writtened() {
                let response = me.data.processing_bytes();
                //println!("response processing bytes:{}", response.available());
                let (found, num) = me.parser.parse_response(&response);
                if !found {
                    break;
                }
                let seq = me.seq;
                me.cond.on_response(seq, response);
                me.data.advance_processed(num);
                me.seq += 1;
            }
        }
    }
}
