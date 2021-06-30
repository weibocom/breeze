// 请求引用结构，避免按位复制，不是单纯的key，而是带请求cmd
pub struct RequestRef {
    ptr: usize,
    len: usize,
}

impl RequestRef {
    pub fn from(ptr: usize, len: usize) -> Self {
        Self { ptr, len }
    }

    pub fn validate(&self) -> bool {
        self.ptr > 0 && self.len > 0
    }

    pub fn ptr(&self) -> usize {
        self.ptr
    }

    pub fn len(&self) -> usize {
        self.len
    }

    // #[inline]
    // pub fn into_data(self) -> &[u8] {
    //     let ptr = self.req_ptr as *const u8;
    //     let data = unsafe { std::slice::from_raw_parts(ptr, self.req_len) };

    //     return data;
    // }
}
