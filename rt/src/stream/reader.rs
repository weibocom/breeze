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
    #[inline(always)]
    pub(crate) fn check(&self) -> Result<()> {
        if self.n > 0 {
            Ok(())
        } else {
            Err(Error::Eof)
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
        self.b += buf.len();
        let mut rb = ReadBuf::new(buf);
        let out = Pin::new(&mut self.client).poll_read(&mut self.cx, &mut rb);
        let r = rb.capacity() - rb.remaining();
        if r > 0 {
            log::debug!("{} bytes received", r);
        }
        self.n += r;

        (r, out)
    }
}
