use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};

use ds::BuffRead;
use protocol::{Error, Result, StreamContext};

pub(crate) struct Reader<'a, 'b, C> {
    n: usize, // 成功读取的数据
    b: usize, // 可以读取的字节数（即buffer的大小）
    client: &'a mut C,
    cx: &'a mut Context<'b>,
}

impl<'a, 'b, C> Reader<'a, 'b, C> {
    #[inline]
    pub(crate) fn from(client: &'a mut C, cx: &'a mut Context<'b>) -> Self {
        let n = 0;
        let b = 0;
        Self { n, client, cx, b }
    }
    // 如果eof了，则返回错误，否则返回读取的num数量
    #[inline(always)]
    pub(crate) fn check(&self) -> Result<()> {
        if self.n > 0 {
            Ok(())
        } else {
            if self.b == 0 {
                log::warn!("+++ in reader.check BufferFull, n:{}, b:{}", self.n, self.b);
                Err(Error::BufferFull)
            } else {
                log::warn!("+++ in reader.check Eof, n:{}, b:{}", self.n, self.b);
                Err(Error::Eof)
            }
        }
    }
}

impl<'a, 'b, C> BuffRead for Reader<'a, 'b, C>
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
        *n += r;

        (r, out)
    }
}
use ds::{GuardedBuffer, MemGuard, RingSlice};
// 已写入未处理的数据流。
pub struct StreamGuard {
    ctx: StreamContext,
    buf: GuardedBuffer,
}
impl protocol::Stream for StreamGuard {
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
    fn context(&mut self) -> &mut StreamContext {
        &mut self.ctx
    }
    // #[inline]
    // fn reserved_hash(&mut self) -> &mut ReservedHash {
    //     &mut self.reserved_hash
    // }
    #[inline]
    fn reserve(&mut self, r: usize) {
        self.buf.grow(r);
    }
}
impl From<GuardedBuffer> for StreamGuard {
    #[inline]
    fn from(buf: GuardedBuffer) -> Self {
        Self {
            buf,
            ctx: Default::default(),
            // reserved_hash: None,
        }
    }
}
impl StreamGuard {
    #[inline]
    pub fn init(init: usize) -> Self {
        // buffer最大从4M调整到64M，观察CPU、Mem fishermen 2022.5.23
        let min = crate::MIN_BUFFER_SIZE;
        let max = crate::MAX_BUFFER_SIZE;
        Self::with(min, max, init.min(max))
    }
    #[inline]
    pub fn new() -> Self {
        // 初始化为0，延迟分配内存
        Self::init(0)
    }
    #[inline]
    fn with(min: usize, max: usize, init: usize) -> Self {
        Self::from(GuardedBuffer::new(min, max, init))
    }
    //#[inline]
    //pub fn raw_init(init: usize) -> Self {
    //    let min = init / 2;
    //    let max = 64 << 20;
    //    Self::from(GuardedBuffer::new(min, max, init, move |_old, _delta| {}))
    //}
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
    #[inline]
    pub fn shrink(&mut self) {
        self.buf.shrink();
    }
    #[inline]
    pub fn cap(&self) -> usize {
        self.buf.cap()
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for StreamGuard {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ctx:{:?} {}", self.ctx, self.buf)
    }
}
impl Debug for StreamGuard {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ctx:{:?} StreamGuard :{}", self.ctx, self.buf)
    }
}

pub struct StreamGuardWithWriter<'a, W> {
    stream: &'a mut StreamGuard,
    writer: &'a mut W,
}

impl<'a, W> Stream for StreamGuardWithWriter<'a, W> {
    #[inline]
    fn take(&mut self, n: usize) -> MemGuard {
        self.stream.take(n)
    }
    #[inline]
    fn len(&self) -> usize {
        self.stream.len()
    }
    #[inline]
    fn slice(&self) -> RingSlice {
        self.stream.slice()
    }
    #[inline]
    fn context(&mut self) -> &mut u64 {
        self.stream.context()
    }
    #[inline]
    fn reserved_hash(&mut self) -> &mut i64 {
        self.stream.reserved_hash()
    }
    #[inline]
    fn reserve(&mut self, r: usize) {
        self.stream.reserve(r);
    }
}

impl<'a, W> StreamWithWriter for StreamGuardWithWriter<'a, W>
where
    W: Writer,
{
    fn write(&mut self, data: &[u8]) -> Result<()> {
        self.writer.write(data)
    }
}
