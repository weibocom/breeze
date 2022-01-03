use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use ds::BuffRead;
use protocol::{Error, Result};

pub(crate) struct Reader<'a, C> {
    n: usize, // 成功读取的数据
    b: usize, // 可以读取的字节数（即buffer的大小）
    client: &'a mut C,
    cx: &'a mut Context<'a>,
}

impl<'a, C> Reader<'a, C> {
    #[inline(always)]
    pub(crate) fn from(client: &'a mut C, cx: &'a mut Context<'a>) -> Self {
        let n = 0;
        let b = 0;
        Self { n, client, cx, b }
    }
    // 如果eof了，则返回错误，否则返回读取的num数量
    #[inline(always)]
    pub(crate) fn check_eof_num(&self) -> Result<usize> {
        // buffer不够，有读取的数据，则认定为流未结束。
        if self.n > 0 || self.b == 0 {
            Ok(self.n)
        } else {
            Err(Error::ReadEof)
        }
    }
}

impl<'a, C> BuffRead for Reader<'a, C>
where
    C: AsyncRead + Unpin,
{
    type Out = Poll<std::io::Result<()>>;
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> (usize, Self::Out) {
        let Self { n, client, cx, b } = self;
        *b += buf.len();
        let mut rb = ReadBuf::new(buf);
        let out = Pin::new(&mut **client).poll_read(cx, &mut rb);
        let r = rb.capacity() - rb.remaining();
        if r > 0 {
            log::info!("{} bytes received ==> {:?}", r, &buf[0..r]);
        }
        *n += r;

        (r, out)
    }
}
use ds::{GuardedBuffer, MemGuard, RingSlice};
// 已写入未处理的数据流。
pub struct StreamGuard {
    ctx: u64,
    pub(crate) buf: GuardedBuffer,
}
impl protocol::Stream for StreamGuard {
    #[inline(always)]
    fn update(&mut self, idx: usize, val: u8) {
        self.buf.update(idx, val);
    }
    #[inline(always)]
    fn at(&self, idx: usize) -> u8 {
        self.buf.at(idx)
    }
    #[inline(always)]
    fn take(&mut self, n: usize) -> MemGuard {
        self.buf.take(n)
    }
    #[inline(always)]
    fn len(&self) -> usize {
        self.buf.len()
    }
    #[inline(always)]
    fn slice(&self) -> RingSlice {
        self.buf.read()
    }
    #[inline(always)]
    fn context(&mut self) -> &mut u64 {
        &mut self.ctx
    }
}
impl From<GuardedBuffer> for StreamGuard {
    #[inline]
    fn from(buf: GuardedBuffer) -> Self {
        Self { buf, ctx: 0 }
    }
}
impl StreamGuard {
    #[inline]
    pub fn pending(&self) -> usize {
        self.buf.pending()
    }
    #[inline]
    pub fn gc(&mut self) {
        self.buf.gc()
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for StreamGuard {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "StreamGuard :{} ", self.buf)
    }
}
