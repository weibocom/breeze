/// Request对象在从stream::io::Client::poll_next生成，
/// 同一个连接，在下一次调用该方法前，Request的内存不会被释放，不会被覆盖，是安全的
use ds::Slice;

pub const MAX_REQUEST_SIZE: usize = 1024 * 1024;

#[derive(Default, Clone, Copy)]
pub struct Request {
    inner: Slice,
}

impl Request {
    pub fn from(data: &[u8]) -> Self {
        Self {
            inner: Slice::from(data),
        }
    }
}
use std::ops::Deref;
impl Deref for Request {
    type Target = Slice;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl AsRef<Slice> for Request {
    #[inline(always)]
    fn as_ref(&self) -> &Slice {
        &self.inner
    }
}
unsafe impl Send for Request {}
unsafe impl Sync for Request {}
