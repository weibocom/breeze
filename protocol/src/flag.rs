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
    #[inline(always)]
    pub fn from_op(op_code: OpCode, op: Operation) -> Self {
        Self {
            op_code,
            op,
            ..Default::default()
        }
    }

    #[inline(always)]
    pub fn new() -> Self {
        Self::default()
    }
    #[inline(always)]
    pub fn set_status_ok(&mut self, ok: bool) {
        debug_assert_eq!(self.ok(), false);
        self.status_ok = ok;
    }
    #[inline(always)]
    pub fn ok(&self) -> bool {
        self.status_ok
    }
    #[inline(always)]
    pub fn set_sentonly(&mut self) {
        debug_assert!(!self.sentonly());
        self.sentonly = true;
    }
    #[inline(always)]
    pub fn sentonly(&self) -> bool {
        self.sentonly
    }
    #[inline(always)]
    pub fn operation(&self) -> Operation {
        self.op
    }
    #[inline(always)]
    pub fn op_code(&self) -> OpCode {
        self.op_code
    }
    #[inline(always)]
    pub fn set_noforward(&mut self) {
        debug_assert!(!self.noforward());
        self.noforward = true;
    }
    #[inline(always)]
    pub fn noforward(&self) -> bool {
        self.noforward
    }

    #[inline(always)]
    pub fn mark(&mut self, bit: u8) {
        self.v |= 1 << bit;
    }
    #[inline(always)]
    pub fn marked(&self, bit: u8) -> bool {
        let m = 1 << bit;
        self.v & m == m
    }
    #[inline(always)]
    pub fn reset_flag(&mut self, op_code: OpCode, op: Operation) {
        self.op_code = op_code;
        self.op = op;
    }
    #[inline(always)]
    pub fn set_ext(&mut self, ext: u64) {
        self.v = ext;
    }
    #[inline(always)]
    pub fn ext(&self) -> u64 {
        self.v
    }
    #[inline(always)]
    pub fn ext_mut(&mut self) -> &mut u64 {
        &mut self.v
    }
}
