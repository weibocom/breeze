use crate::{AsyncWriteAll, Request, MAX_REQUEST_SIZE};

use protocol::Protocol;

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
    bytes: u64,   // 一共复制的字节数量
    req_num: u64, // 发送的请求数量
    parsed: bool,
    parsed_idx: usize,
    read_done: bool,
}

impl Receiver {
    pub fn new() -> Self {
        log::debug!("Receiver inited");
        let init_cap = 2048usize;
        Self {
            buff: vec![0; init_cap],
            w: 0,
            cap: init_cap,
            r: 0,
            bytes: 0,
            req_num: 0,
            parsed: false,
            parsed_idx: 0,
            read_done: false,
        }
    }
    pub fn poll_copy<R, W, P>(
        &mut self,
        cx: &mut Context,
        mut reader: Pin<&mut R>,
        mut writer: Pin<&mut W>,
        parser: &P,
    ) -> Poll<Result<(u64, u64)>>
    where
        R: AsyncRead + ?Sized,
        P: Protocol + Unpin,
        W: AsyncWriteAll + ?Sized,
    {
        while !self.read_done {
            if self.w == self.cap {
                self.extends()?;
            }
            if !self.parsed {
                let mut buff = ReadBuf::new(&mut self.buff[self.w..]);
                ready!(reader.as_mut().poll_read(cx, &mut buff))?;
                let read = buff.filled().len();
                if read == 0 {
                    self.read_done = true;
                }
                self.w += read;
                self.bytes += read as u64;
                let (parsed, n) = parser.parse_request(&self.buff[self.r..self.w])?;
                self.parsed = parsed;
                self.parsed_idx = self.r + n;
            }
            if !self.parsed {
                continue;
            }
            debug_assert!(self.parsed_idx > self.r);
            let req = Request::from(&self.buff[self.r..self.parsed_idx]);
            ready!(writer.as_mut().poll_write(cx, req))?;
            log::debug!("io:request recived len:{}", self.parsed_idx - self.r);
            self.req_num += 1;
            self.parsed = false;
            if self.r == self.w {
                self.r = 0;
                self.w = 0;
            }
        }
        if self.w > self.r {
            // 有数据未写完，未解析完成，但reader已关闭
            if !self.parsed {
                return Poll::Ready(Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "read stream closed, but parser is false.",
                )));
            }
            debug_assert!(self.parsed_idx > self.r);
            let req = Request::from(&self.buff[self.r..self.parsed_idx]);
            ready!(writer.as_mut().poll_write(cx, req))?;
        }
        Poll::Ready(Ok((self.bytes, self.req_num)))
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
