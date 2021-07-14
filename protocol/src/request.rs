/// Request对象在从stream::io::Client::poll_next生成，
/// 同一个连接，在下一次调用该方法前，Request的内存不会被释放，不会被覆盖，是安全的
use ds::Slice;

pub const MAX_REQUEST_SIZE: usize = 1024 * 1024;

use std::sync::Arc;

#[derive(Default, Clone)]
pub struct Request {
    // TODO just for debug
    pub inner: Slice,
    id: RequestId,
    // 是否需要返回结果
    noreply: bool,
    // 如果是from_vec调用的，则Request在释放的时候要确保内存被释放
    // 访问数据要直接使用slice访问，不能使用_data。因为_data可能为EMPTY
    _data: Arc<Vec<u8>>,
}

impl Request {
    pub fn from(data: &[u8], id: RequestId) -> Self {
        Self {
            inner: Slice::from(data),
            id: id,
            noreply: false,
            _data: Arc::new(Vec::new()),
        }
    }
    pub fn from_vec(data: Vec<u8>, id: RequestId) -> Self {
        Self {
            inner: Slice::from(&data),
            id: id,
            noreply: false,
            _data: Arc::new(data),
        }
    }
    pub fn id(&self) -> &RequestId {
        &self.id
    }
    pub fn set_noreply(&mut self) {
        self.noreply = true;
    }
    pub fn noreply(&self) -> bool {
        self.noreply
    }
    pub fn data(&self) -> &[u8] {
        return self.inner.data();
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
    #[inline(always)]
    pub fn from(session_id: usize, seq: usize) -> Self {
        Self { session_id, seq }
    }
    #[inline(always)]
    pub fn incr(&mut self) {
        self.seq += 1;
    }
}
