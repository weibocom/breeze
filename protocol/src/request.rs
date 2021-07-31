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

    _data: Arc<Vec<u8>>,
}

impl Request {
    #[inline(always)]
    pub fn from(data: &[u8], id: RequestId) -> Self {
        Self {
            inner: Slice::from(data),
            id: id,
            noreply: false,
            _data: Default::default(),
        }
    }
    #[inline(always)]
    pub fn from_vec(data: Vec<u8>, id: RequestId) -> Self {
        Self {
            inner: Slice::from(&data),
            id: id,
            noreply: false,
            _data: Arc::new(data),
        }
    }
    #[inline(always)]
    pub fn id(&self) -> RequestId {
        self.id
    }
    #[inline(always)]
    pub fn set_noreply(&mut self) {
        self.noreply = true;
    }
    #[inline(always)]
    pub fn noreply(&self) -> bool {
        self.noreply
    }
    #[inline(always)]
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

pub use rid::RequestId;

#[cfg(debug_assertions)]
mod rid {
    #[derive(Default, Debug, PartialEq, Clone, Copy)]
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
}

#[cfg(not(debug_assertions))]
mod rid {
    #[derive(Default, Debug, PartialEq, Clone, Copy)]
    pub struct RequestId;
    impl RequestId {
        #[inline(always)]
        pub fn from(_session_id: usize, _seq: usize) -> Self {
            Self
        }
        #[inline(always)]
        pub fn incr(&mut self) {}
    }
}
