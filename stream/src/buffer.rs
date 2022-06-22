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
    #[inline]
    pub(crate) fn from(client: &'a mut C, cx: &'a mut Context<'a>) -> Self {
        let n = 0;
        let b = 0;
        Self { n, client, cx, b }
    }
    // 如果eof了，则返回错误，否则返回读取的num数量
    #[inline]
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
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> (usize, Self::Out) {
        let Self { n, client, cx, b } = self;
        *b += buf.len();
        let mut rb = ReadBuf::new(buf);
        let out = Pin::new(&mut **client).poll_read(cx, &mut rb);
        let r = rb.capacity() - rb.remaining();
        if r > 0 {
            log::debug!("{} bytes received ==> {:?}", r, &buf[0..r]);
        }
        *n += r;

        (r, out)
    }
}
use ds::{GuardedBuffer, MemGuard, RingSlice};
// 已写入未处理的数据流。
pub struct StreamGuard {
    ctx: u64,
    reserved_hash: i64,
    buf: GuardedBuffer,
}
impl protocol::Stream for StreamGuard {
    //#[inline]
    //fn update(&mut self, idx: usize, val: u8) {
    //    self.buf.update(idx, val);
    //}
    //#[inline]
    //fn at(&self, idx: usize) -> u8 {
    //    self.buf.at(idx)
    //}
    #[inline]
    fn take(&mut self, n: usize) -> MemGuard {
        self.buf.take(n)
    }
    #[inline]
    fn len(&self) -> usize {
        self.buf.len()
    }
    #[inline]
    fn slice(&self) -> RingSlice {
        self.buf.read()
    }
    #[inline]
    fn context(&mut self) -> &mut u64 {
        &mut self.ctx
    }
    #[inline]
    fn reserved_hash(&mut self) -> &mut i64 {
        &mut self.reserved_hash
    }
}
impl From<GuardedBuffer> for StreamGuard {
    #[inline]
    fn from(buf: GuardedBuffer) -> Self {
        Self {
            buf,
            ctx: 0,
            reserved_hash: 0,
        }
    }
}
impl StreamGuard {
    #[inline]
    pub fn init(init: usize) -> Self {
        const MIN: usize = 1024;
        // buffer最大从4M调整到64M，观察CPU、Mem fishermen 2022.5.23
        const MAX: usize = 64 << 20;
        let init = init.max(MIN).min(MAX);
        Self::with(MIN, MAX, init)
    }
    #[inline]
    pub fn new() -> Self {
        Self::init(1024)
    }
    #[inline]
    fn with(min: usize, max: usize, init: usize) -> Self {
        let mut buf_rx = metrics::Path::base().num("mem_buf_rx");
        Self::from(GuardedBuffer::new(min, max, init, move |_old, delta| {
            // TODO 对大于2M的场景记录日志观察 fishermen 2022.5.25
            if _old >= 2 << 20 {
                log::warn!("mem size {} will add {}", _old, delta);
            }
            buf_rx += delta;
        }))
    }
    #[inline]
    pub fn pending(&self) -> usize {
        self.buf.pending()
    }
    #[inline]
    pub fn try_gc(&mut self) -> bool {
        self.buf.gc();
        self.pending() == 0
    }
    #[inline]
    pub fn write<R, O>(&mut self, r: &mut R) -> O
    where
        R: BuffRead<Out = O>,
    {
        self.buf.write(r)
    }
    //#[inline]
    //pub fn cap(&self) -> usize {
    //    self.buf.cap()
    //}
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for StreamGuard {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ctx:{} StreamGuard :{}", self.ctx, self.buf,)
    }
}
impl Debug for StreamGuard {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ctx:{} StreamGuard :{}", self.ctx, self.buf)
    }
}
