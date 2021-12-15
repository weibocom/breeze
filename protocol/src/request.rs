use crate::Operation;
/// Request对象在从stream::io::Client::poll_next生成，
/// 同一个连接，在下一次调用该方法前，Request的内存不会被释放，不会被覆盖，是安全的
use ds::Slice;

pub const MAX_REQUEST_SIZE: usize = 1024 * 1024;

use std::sync::Arc;

#[derive(Debug)]
pub struct Token {
    // meta postion in multibulk，对于slice里说，pos=meta_pos+meta_len
    pub meta_pos: usize,
    // meta len，长度的长度
    pub meta_len: usize,
    // token data position，有效信息位置
    pub pos: usize,
    // token data len，有效信息的长度
    pub len: usize,
}

#[derive(Default, Clone)]
pub struct Request {
    noreply: bool,
    op: Operation,
    keys: Vec<Slice>,
    inner: Slice,
    id: RequestId,
    //tokens: Vec<Token>,
    tokens: Vec<String>,

    // 如果内存是由Request管理的，则将data交由_data，避免copy成本。
    // 如果不是，里面存储的是Vec::EMPTY，这个clone是零开销的，本身不占用内存。
    _data: Arc<Vec<u8>>,
}

impl Request {
    #[inline(always)]
    pub fn from(data: Slice, op: Operation, keys: Vec<Slice>) -> Self {
        Self {
            inner: data,
            op: op,
            keys: keys,
            ..Default::default()
        }
    }
    #[inline(always)]
    pub fn from_request(data: Vec<u8>, keys: Vec<Slice>, req: &Request) -> Self {
        Self {
            inner: Slice::from(&data),
            id: req.id,
            noreply: req.noreply,
            _data: Arc::new(data),
            keys: keys,
            op: req.op,
            tokens: Default::default(),
        }
    }
    // request 自己接管data，构建一个全新的request
    pub fn from_data(
        data: Vec<u8>,
        keys: Vec<Slice>,
        id: RequestId,
        noreply: bool,
        op: Operation,
    ) -> Self {
        Self {
            inner: Slice::from(&data),
            id: id,
            noreply: noreply,
            _data: Arc::new(data),
            keys: keys,
            op: op,
            tokens: Default::default(),
        }
    }
    #[inline(always)]
    pub fn operation(&self) -> Operation {
        self.op
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
    #[inline(always)]
    pub fn set_request_id(&mut self, id: RequestId) {
        self.id = id;
    }
    #[inline(always)]
    pub fn keys(&self) -> &[Slice] {
        &self.keys
    }
    #[inline(always)]
    pub fn last_key(&self) -> &Slice {
        debug_assert!(self.keys.len() > 0);
        unsafe { &self.keys.get_unchecked(self.keys.len() - 1) }
    }
    #[inline(always)]
    pub fn set_tokens(&mut self, tokens: Vec<String>) {
        self.tokens = tokens;
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
        metric_id: usize,
    }

    impl RequestId {
        #[inline(always)]
        pub fn from(session_id: usize, seq: usize, metric_id: usize) -> Self {
            Self {
                session_id,
                seq,
                metric_id,
            }
        }
        #[inline(always)]
        pub fn incr(&mut self) {
            self.seq += 1;
        }
        #[inline(always)]
        pub fn metric_id(&self) -> usize {
            self.metric_id
        }
        #[inline(always)]
        pub fn session_id(&self) -> usize {
            self.session_id
        }
    }
    use std::fmt;
    impl fmt::Display for RequestId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(
                f,
                "(rid: {} => {} metric:{})",
                self.session_id, self.seq, self.metric_id
            )
        }
    }
}

#[cfg(not(debug_assertions))]
mod rid {
    use std::fmt;
    #[derive(Default, Debug, PartialEq, Clone, Copy)]
    pub struct RequestId {
        metric_id: usize,
    }
    impl RequestId {
        #[inline(always)]
        pub fn from(_session_id: usize, _seq: usize, metric_id: usize) -> Self {
            Self { metric_id }
        }
        #[inline(always)]
        pub fn incr(&mut self) {}
        #[inline(always)]
        pub fn metric_id(&self) -> usize {
            self.metric_id
        }
    }

    impl fmt::Display for RequestId {
        fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
            Ok(())
        }
    }
}

use std::fmt::{self, Display, Formatter};
impl Display for Request {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(len:{} keys:{} data:{:?} {})",
            self.len(),
            self.keys.len(),
            self.data(),
            self.id
        )
    }
}
use std::fmt::Debug;
impl Debug for Request {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(len:{} keys:{} data:{:?} {})",
            self.len(),
            self.keys.len(),
            self.data(),
            self.id
        )
    }
}
