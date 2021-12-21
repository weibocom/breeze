use std::collections::VecDeque;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use futures::ready;
use tokio::io::{AsyncRead, ReadBuf};

use ds::ResizedRingBuffer;
use metrics::MetricName;
use protocol::{Protocol, Request, RequestId};

use super::IoMetric;
use crate::AsyncWriteAll;

pub(super) struct Receiver {
    buff: ResizedRingBuffer,
    req: Option<Request>,
    poll_times: usize,
    last_log_time: Instant,
}

impl Receiver {
    pub fn new(metric_id: usize) -> Self {
        let buff = ResizedRingBuffer::new(move |old, delta| {
            if delta > old as isize && delta >= 32 * 1024 {
                log::info!("buffer resized ({}, {}). {}", old, delta, metric_id.name());
            }
            metrics::count("mem_buff_rx", delta, metric_id);
        });
        Self { buff, req: None, poll_times: 0, last_log_time: Instant::now() }
    }
    // 返回当前请求的size，以及请求的类型。
    #[inline]
    pub fn poll_copy_one<R, W, P>(
        &mut self,
        addr: String,
        cx: &mut Context,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
        parser: &P,
        rid: &RequestId,
        metric: &mut IoMetric,
        direct_response_queue: &mut VecDeque<Request>,
        request: &mut Option<Vec<u8>>,
    ) -> Poll<Result<()>>
    where
        R: AsyncRead + ?Sized,
        P: Protocol + Unpin,
        W: AsyncWriteAll + ?Sized,
    {
        self.poll_times += 1;
        if self.last_log_time.elapsed() >= Duration::from_secs(60) {
            log::info!("recv from client: {} poll times: {}", addr, self.poll_times);
            self.poll_times = 0;
            self.last_log_time = Instant::now();
        }
        log::debug!("buff:{} rid:{}", self.buff, rid);
        while self.req.is_none() {
            if self.buff.len() > 0 {
                //  每一次ping-pong之后，都会通过reset将read重置为0.
                //  确保RingSlice可以转换成唯一一个Slice
                debug_assert_eq!(self.buff.read(), 0);
                let mut slices = self.buff.data().as_slices();
                debug_assert_eq!(slices.len(), 1);
                self.req = parser.parse_request(slices.pop().expect("request slice"))?;
                if self.req.is_some() {
                    break;
                }
            }
            // 说明当前已经没有处理中的完整的请求。内存可以安全的move，
            // 不会导致已有的request，因为move，导致请求失败或者panic.
            // 数据不足。要从stream中读取
            let mut buff = ReadBuf::new(self.buff.as_mut_bytes());
            ready!(reader.as_mut().poll_read(cx, &mut buff))?;
            let read = buff.filled().len();
            if read == 0 {
                if self.buff.len() > 0 {
                    log::warn!("eof, but {} bytes left.", self.buff.len());
                }
                metric.reset();
                return Poll::Ready(Ok(()));
            }
            self.buff.advance_write(read);
            metric.req_received(read);
            log::debug!("{} bytes received, session_id = {}", read, rid.session_id());
        }
        // 到这req一定存在，不用take+unwrap是为了在出现pending的时候，不重新insert
        if let Some(ref mut req) = self.req {
            req.set_request_id(*rid);
            metric.req_done(req.operation(), req.len(), req.keys().len());
            unsafe {
                log::debug!(
                    "++++ parsed client req/{:?}:  {} => {:?}",
                    req.id(),
                    self.buff,
                    String::from_utf8_unchecked(req.data().to_vec())
                );
            }
            if parser.is_direct_response(req) {
                direct_response_queue.push_back(req.clone());
            }
            else {
                ready!(writer.as_mut().poll_write(cx, &req))?;
            }
            self.buff.advance_read(req.len());
        }
        // 把read重置为0.避免ringbuffer形成ring。
        self.buff.reset_read();
        request.replace(self.req.take().unwrap().to_vec());
        Poll::Ready(Ok(()))
    }
}
