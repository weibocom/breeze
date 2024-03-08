mod order;
pub use order::*;

mod buffer;
pub use buffer::*;

use crate::{Range, Slicer};
// 把一个Slicer的部分切片写入到一个Writer中。必须全部写入成功，否则返回错误。
pub trait Writer {
    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()>;
    #[inline(always)]
    fn write_r<S: Slicer, R: Range>(&mut self, r: R, slicer: &S) -> std::io::Result<()> {
        slicer.with_seg(r, |seg, _oft, _seg| self.write_all(seg))
    }
}

impl Writer for Vec<u8> {
    #[inline]
    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.reserve(data.len());
        use std::ptr::copy_nonoverlapping as copy;
        unsafe {
            let len = self.len();
            let ptr = self.as_mut_ptr().add(len);
            copy(data.as_ptr(), ptr, data.len());
            self.set_len(len + data.len());
        }
        Ok(())
    }
    #[inline(always)]
    fn write_r<S: Slicer, R: Range>(&mut self, r: R, slicer: &S) -> std::io::Result<()> {
        self.reserve(r.len(slicer));
        slicer.with_seg(r, |seg, _oft, _seg| self.write_all(seg))
    }
}
