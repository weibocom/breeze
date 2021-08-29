use crate::AsyncWriteAll;

use protocol::{Protocol, Request, RequestId, MAX_REQUEST_SIZE};

use futures::ready;

use super::IoMetric;
use tokio::io::{AsyncRead, ReadBuf};

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
pub(super) struct Receiver {
    r: usize, // 上一个request的读取位置
    w: usize, // 当前buffer写入的位置
    cap: usize,
    buff: Vec<u8>,
    req: Option<Request>,
}

impl Receiver {
    pub fn new() -> Self {
        let init_cap = 4 * 1024;
        Self {
            buff: vec![0; init_cap],
            w: 0,
            cap: init_cap,
            r: 0,
            req: None,
        }
    }
    #[inline(always)]
    fn reset(&mut self) {
        if self.w == self.r {
            self.r = 0;
            self.w = 0;
        }
    }
    // 返回当前请求的size，以及请求的类型。
    pub fn poll_copy_one<R, W, P>(
        &mut self,
        cx: &mut Context,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
        parser: &P,
        rid: &RequestId,
        metric: &mut IoMetric,
    ) -> Poll<Result<()>>
    where
        R: AsyncRead + ?Sized,
        P: Protocol + Unpin,
        W: AsyncWriteAll + ?Sized,
    {
        log::debug!("r:{} w:{} ", self.r, self.w);
        while self.req.is_none() {
            if self.w > self.r {
                self.req = parser.parse_request(&self.buff[self.r..self.w])?;
                if self.req.is_some() {
                    break;
                }
            }
            // 说明当前已经没有处理中的完整的请求。内存可以安全的move，
            // 不会导致已有的request，因为move，导致请求失败或者panic.
            self.try_extends()?;
            // 数据不足。要从stream中读取
            let mut buff = ReadBuf::new(&mut self.buff[self.w..]);
            ready!(reader.as_mut().poll_read(cx, &mut buff))?;
            let read = buff.filled().len();
            if read == 0 {
                if self.w > self.r {
                    log::warn!("eof, but {} bytes left.", self.w - self.r);
                }
                metric.reset();
                return Poll::Ready(Ok(()));
            }
            self.w += read;
            metric.req_received(read);
            log::debug!("{} bytes received.{}", read, rid);
        }
        // 到这req一定存在，不用take+unwrap是为了在出现pending的时候，不重新insert
        if let Some(ref mut req) = self.req {
            req.set_request_id(*rid);
            metric.req_done(req.operation(), req.len());
            log::debug!("parsed: {}=>{} {}", self.r, req.len(), rid);
            ready!(writer.as_mut().poll_write(cx, &req))?;
            self.r += req.len();
        }
        self.reset();
        self.req.take();
        Poll::Ready(Ok(()))
    }

    // 调用者需要确保，extends之前，所有完整的请求都已完成处理。不存在处理中的请求，避免内存指向错误, 导致数据异常或者panic。
    // 如果r > 0. 先进行一次move。通过是client端发送的是pipeline请求时会出现这种情况
    // 每次扩容两倍，最多扩容1MB，超过1MB就会扩容失败
    #[inline(always)]
    fn try_extends(&mut self) -> Result<()> {
        if self.w != self.cap {
            return Ok(());
        }
        if self.r == self.w {
            log::info!("extends r == w. r:{} w:{}", self.r, self.w);
            self.reset();
            return Ok(());
        }
        if self.r > 0 {
            self.move_data();
            Ok(())
        } else {
            self.extend()
        }
    }
    #[inline]
    fn move_data(&mut self) {
        debug_assert!(self.r > 0);
        // 有可能是因为pipeline，所以上一次request结束后，可能还有未处理的请求数据
        // 把[r..w]的数据copy到最0位置
        log::info!("move: r:{} w:{} ", self.r, self.w);
        use std::ptr::copy;
        let len = self.w - self.r;
        debug_assert!(len > 0);
        unsafe {
            copy(
                self.buff.as_ptr().offset(self.r as isize),
                self.buff.as_mut_ptr(),
                len,
            );
        }
        self.r = 0;
        self.w = len;
    }
    #[inline]
    fn extend(&mut self) -> Result<()> {
        log::info!(
            "extend: r:{} w:{} cap: from {} to {}",
            self.r,
            self.w,
            self.cap,
            2 * self.cap,
        );
        if self.cap >= MAX_REQUEST_SIZE {
            log::warn!("request size limited:{} >= {}", self.cap, MAX_REQUEST_SIZE);
            Err(Error::new(
                ErrorKind::InvalidInput,
                "max request size limited: 1mb",
            ))
        } else {
            // 说明容量不足。需要扩展
            let cap = self.buff.len();
            let cap = (cap * 2).min(MAX_REQUEST_SIZE);
            let mut new_buff = vec![0u8; cap];
            use std::ptr::copy_nonoverlapping as copy;
            unsafe { copy(self.buff.as_ptr(), new_buff.as_mut_ptr(), self.buff.len()) };
            // 如果还存在处理中的请求，buff clear掉之后可能指向一段已释放的内存。
            self.buff.clear();
            self.buff = new_buff;
            self.cap = cap;
            Ok(())
        }
    }
}
