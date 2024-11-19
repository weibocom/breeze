use crate::{HashedCommand, OpCode, Operation};
pub type FlagExt = u64;
#[derive(Debug, Default)]
pub struct Flag {
    op_code: OpCode,
    op: Operation,
    sentonly: bool,
    noforward: bool,
    can_split: bool,
    v: FlagExt,
}

use ds::Ext;
impl Ext for Flag {
    #[inline]
    fn ext(&self) -> FlagExt {
        self.v
    }
    #[inline]
    fn ext_mut(&mut self) -> &mut FlagExt {
        &mut self.v
    }
}
impl Ext for HashedCommand {
    #[inline]
    fn ext(&self) -> FlagExt {
        self.flag().ext()
    }
    #[inline]
    fn ext_mut(&mut self) -> &mut FlagExt {
        self.flag_mut().ext_mut()
    }
}

impl Flag {
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
    pub fn set_sentonly(&mut self, sentonly: bool) -> &mut Self {
        self.sentonly = sentonly;
        self
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
    pub fn set_noforward(&mut self, noforward: bool) -> &mut Self {
        debug_assert!(!self.noforward());
        self.noforward = noforward;
        self
    }
    #[inline]
    pub fn noforward(&self) -> bool {
        self.noforward
    }
    #[inline]
    pub fn reset_flag(&mut self, op_code: OpCode, op: Operation) {
        self.op_code = op_code;
        self.op = op;
    }
    #[inline]
    pub fn can_split(&self) -> bool {
        self.can_split
    }
    #[inline]
    pub fn set_can_split(&mut self) {
        self.can_split = true;
    }
}

#[derive(Debug, Default)]
pub struct ResponseHeader {
    pub(crate) header: Vec<u8>,
    pub(crate) rows: u16,    // 包含的响应行数
    pub(crate) columns: u16, // 每行的响应列数
}

impl ResponseHeader {
    #[inline]
    pub fn new(header: Vec<u8>, rows: u16, columns: u16) -> Self {
        ResponseHeader {
            header,
            rows,
            columns,
        }
    }
}
