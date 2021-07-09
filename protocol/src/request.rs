/// Request对象在从stream::io::Client::poll_next生成，
/// 同一个连接，在下一次调用该方法前，Request的内存不会被释放，不会被覆盖，是安全的
use ds::Slice;

pub const MAX_REQUEST_SIZE: usize = 1024 * 1024;

#[derive(Default, Clone)]
pub struct Request {
    // TODO just for debug
    pub inner: Slice,
    id: RequestId,
}

impl Request {
    pub fn from(data: &[u8], id: RequestId) -> Self {
        Self {
            inner: Slice::from(data),
            id: id,
        }
    }
    pub fn id(&self) -> &RequestId {
        &self.id
    }
    pub fn update_data(&mut self, data: &[u8]) {
        self.inner = Slice::from(data);
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

#[derive(Default, Clone, Debug, PartialEq)]
pub struct RequestId {
    session_id: usize, // 关联至一个client的实际的connection
    seq: usize,        // 自增序列号
}

impl RequestId {
    pub fn from(session_id: usize, seq: usize) -> Self {
        Self { session_id, seq }
    }
    pub fn incr(&mut self) {
        self.seq += 1;
    }
}
