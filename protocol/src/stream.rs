use ds::{MemGuard, RingSlice};
use std::ops::Deref;

pub trait AsyncBufRead {
    fn poll_recv(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<crate::Result<()>>;
}

pub type StreamContext = [u8; 16];

pub trait BufRead {
    fn len(&self) -> usize;
    //fn at(&self, idx: usize) -> u8;
    fn slice(&self) -> ds::RingSlice;
    //fn update(&mut self, idx: usize, val: u8);
    fn take(&mut self, n: usize) -> ds::MemGuard;
    #[inline]
    fn ignore(&mut self, n: usize) {
        let _ = self.take(n);
    }
    // 在解析一个流的不同的req/response时，有时候需要共享数据。
    fn context(&mut self) -> &mut StreamContext;
    fn reserve(&mut self, r: usize);
}
use super::Result;
pub trait Writer: ds::BufWriter + Sized {
    fn cap(&self) -> usize;
    fn pending(&self) -> usize;
    // 写数据，一次写完
    fn write(&mut self, data: &[u8]) -> Result<()>;
    #[inline]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.write(&[v])
    }
    #[inline]
    fn write_u16(&mut self, v: u16) -> Result<()> {
        // let mut data = Vec::with_capacity(2);
        // data.write_u16::<BigEndian>(v)?;
        // self.write(&data[0..])
        self.write(&v.to_be_bytes())
    }
    #[inline]
    fn write_u32(&mut self, v: u32) -> Result<()> {
        // let mut data = Vec::with_capacity(4);
        // data.write_u32::<BigEndian>(v)?;
        // self.write(&data[0..])
        self.write(&v.to_be_bytes())
    }
    #[inline]
    fn write_u64(&mut self, v: u64) -> Result<()> {
        // let mut data = Vec::with_capacity(8);
        // data.write_u64::<BigEndian>(v)?;
        // self.write(&data[0..])
        self.write(&v.to_be_bytes())
    }
    #[inline(always)]
    fn write_str_num(&mut self, v: usize) -> Result<()> {
        use ds::NumStr;
        v.with_str(|b| self.write(b))
    }

    // hint: 提示可能优先写入到cache
    fn cache(&mut self, hint: bool);

    #[inline]
    fn write_slice<S: Deref<Target = MemGuard>>(&mut self, data: &S, oft: usize) -> Result<()> {
        (&*data).copy_to(oft, self)?;
        Ok(())
    }

    // 暂时没发现更好的实现方式，先用这个实现
    #[inline]
    fn write_ringslice(&mut self, data: &RingSlice, oft: usize) -> Result<()> {
        data.copy_to(oft, self)?;
        Ok(())
    }

    fn shrink(&mut self);
    fn try_gc(&mut self) -> bool;
}

pub trait Stream: AsyncBufRead + BufRead + Writer + std::fmt::Debug {}
