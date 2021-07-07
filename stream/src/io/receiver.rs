use crate::AsyncWriteAll;

use protocol::{Protocol, Request, RequestId, MAX_REQUEST_SIZE};

use futures::ready;

use tokio::io::{AsyncRead, ReadBuf};

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
pub(super) struct Receiver {
    // 上一个request的读取位置
    r: usize,
    // 当前buffer写入的位置
    w: usize,
    cap: usize,
    buff: Vec<u8>,
    parsed: bool,
    parsed_idx: usize,
}

impl Receiver {
    pub fn new() -> Self {
        let init_cap = 2048usize;
        Self {
            buff: vec![0; init_cap],
            w: 0,
            cap: init_cap,
            r: 0,
            parsed: false,
            parsed_idx: 0,
        }
    }
    fn reset(&mut self) {
        if self.w == self.r {
            self.r = 0;
            self.w = 0;
        }
        self.parsed_idx = 0;
        self.parsed = false;
    }
    pub fn poll_copy_one<R, W, P>(
        &mut self,
        cx: &mut Context,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
        parser: &P,
        session_id: usize,
        seq: usize,
    ) -> Poll<Result<usize>>
    where
        R: AsyncRead + ?Sized,
        P: Protocol + Unpin,
        W: AsyncWriteAll + ?Sized,
    {
        log::debug!(
            "io-receiver-poll-copy. r:{} idx:{} w:{} ",
            self.r,
            self.parsed_idx,
            self.w
        );
        while !self.parsed {
            if self.w == self.cap {
                self.extends()?;
            }
            let mut buff = ReadBuf::new(&mut self.buff[self.w..]);
            ready!(reader.as_mut().poll_read(cx, &mut buff))?;
            let read = buff.filled().len();
            if read == 0 {
                if self.w > self.r {
                    log::warn!("io-receiver: eof, but {} bytes left.", self.w - self.r);
                }
                return Poll::Ready(Ok(0));
            }
            log::debug!("io-receiver-poll: {} bytes received.", read);
            self.w += read;
            let (parsed, n) = parser.parse_request(&self.buff[self.r..self.w])?;
            self.parsed = parsed;
            self.parsed_idx = self.r + n;
        }
        let id = RequestId::from(session_id, seq);
        let req = Request::from(&self.buff[self.r..self.parsed_idx], id);
        log::debug!(
            "io-receiver-poll: len:{} {:?}",
            self.parsed_idx - self.r,
            &self.buff[self.r..self.parsed_idx.min(48)]
        );
        ready!(writer.as_mut().poll_write(cx, &req))?;
        self.r += req.len();
        self.reset();
        Poll::Ready(Ok(req.len()))
    }
    // 如果r > 0. 先进行一次move。通过是client端发送的是pipeline请求时会出现这种情况
    // 每次扩容两倍，最多扩容1MB，超过1MB就会扩容失败
    fn extends(&mut self) -> Result<()> {
        debug_assert_eq!(self.w, self.cap);
        if self.r > 0 {
            self.move_data();
            Ok(())
        } else {
            if self.cap >= MAX_REQUEST_SIZE {
                Err(Error::new(
                    ErrorKind::ConnectionAborted,
                    "max request size limited: 1mb",
                ))
            } else {
                // 说明容量不足。需要扩展
                let cap = self.buff.len();
                let cap = (cap * 2).min(MAX_REQUEST_SIZE);
                let mut new_buff = vec![0u8; cap];
                use std::ptr::copy_nonoverlapping;
                unsafe {
                    copy_nonoverlapping(self.buff.as_ptr(), new_buff.as_mut_ptr(), self.buff.len())
                };
                self.buff.clear();
                self.buff = new_buff;
                Ok(())
            }
        }
    }
    fn move_data(&mut self) {
        debug_assert!(self.r > 0);
        // 有可能是因为pipeline，所以上一次request结束后，可能还有未处理的请求数据
        // 把[r..w]的数据copy到最0位置
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
}
