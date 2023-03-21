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
    fn write_s_u16(&mut self, v: u16) -> Result<()> {
        if v < ds::NUM_STR_TBL.len() as u16 {
            self.write(ds::NUM_STR_TBL[v as usize].as_bytes())
        } else {
            self.write(v.to_string().as_bytes())
        }
    }

    // hint: 提示可能优先写入到cache
    fn cache(&mut self, hint: bool);

    #[inline]
    fn write_slice(&mut self, data: &ds::RingSlice, oft: usize) -> Result<()> {
        data.copy_to(oft, self)?;
        Ok(())
    }
    fn shrink(&mut self);
    fn try_gc(&mut self) -> bool;
}

pub trait Stream: AsyncBufRead + BufRead + Writer + std::fmt::Debug {}
