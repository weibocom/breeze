use crate::{OpCode, Operation};
#[derive(Debug, Default)]
pub struct Flag {
    op_code: OpCode,
    op: Operation,
    sentonly: bool,
    status_ok: bool,
    noforward: bool,
    v: u64,
}

impl Flag {
    // first = true 满足所有条件1. 当前请求是multiget；2. 拆分了多个子请求；3. 是`第一`个子请求；
    // last  = true 满足所有条件1. 当前请求是multiget；2. 拆分了多个子请求；3. 是`最后`一个子请求；
    #[inline]
    pub fn from_op(op_code: OpCode, op: Operation) -> Self {
        Self {
            op_code,
            op,
            ..Default::default()
        }
    }

    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    #[inline]
    pub fn set_status_ok(&mut self, ok: bool) {
        assert_eq!(self.ok(), false);
        self.status_ok = ok;
    }
    #[inline]
    pub fn ok(&self) -> bool {
        self.status_ok
    }
    #[inline]
    pub fn set_sentonly(&mut self, sentonly: bool) {
        self.sentonly = sentonly;
    }
    #[inline]
    pub fn sentonly(&self) -> bool {
        self.sentonly
    }
    #[inline]
    pub fn operation(&self) -> Operation {
        self.op
    }
    #[inline]
    pub fn op_code(&self) -> OpCode {
        self.op_code
    }
    #[inline]
    pub fn set_noforward(&mut self, noforward: bool) {
        assert!(!self.noforward());
        self.noforward = noforward;
    }
    #[inline]
    pub fn noforward(&self) -> bool {
        self.noforward
    }

    #[inline]
    pub fn mark(&mut self, bit: u8) {
        self.v |= 1 << bit;
    }
    #[inline]
    pub fn marked(&self, bit: u8) -> bool {
        let m = 1 << bit;
        self.v & m == m
    }
    #[inline]
    pub fn reset_flag(&mut self, op_code: OpCode, op: Operation) {
        self.op_code = op_code;
        self.op = op;
    }
    #[inline]
    pub fn set_ext(&mut self, ext: u64) {
        self.v = ext;
    }
    #[inline]
    pub fn ext(&self) -> u64 {
        self.v
    }
    #[inline]
    pub fn ext_mut(&mut self) -> &mut u64 {
        &mut self.v
    }
}
