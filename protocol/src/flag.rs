use crate::{HashedCommand, OpCode, Operation};
pub type FlagExt = u64;
#[derive(Debug, Default)]
pub struct Flag {
    op_code: OpCode,
    op: Operation,
    sentonly: bool,
    noforward: bool,
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

    //#[inline]
    //pub fn mark(&mut self, bit: u8) {
    //    self.v |= 1 << bit;
    //}
    //#[inline]
    //pub fn marked(&self, bit: u8) -> bool {
    //    let m = 1 << bit;
    //    self.v & m == m
    //}
    #[inline]
    pub fn reset_flag(&mut self, op_code: OpCode, op: Operation) {
        self.op_code = op_code;
        self.op = op;
    }
    //#[inline]
    //pub fn set_ext(&mut self, ext: u64) {
    //    self.v = ext;
    //}
    //#[inline]
    //pub fn ext(&self) -> u64 {
    //    self.v
    //}
    //#[inline]
    //pub fn ext_mut(&mut self) -> &mut u64 {
    //    &mut self.v
    //}
}

#[derive(Debug, Clone, Copy)]
pub enum TryNextType {
    NotTryNext = 0,
    TryNext = 1,
    Unknown = 2,
}
// 方便检索
const TRY_NEXT_TABLE: [TryNextType; 3] = [
    TryNextType::NotTryNext,
    TryNextType::TryNext,
    TryNextType::Unknown,
];

impl From<u8> for TryNextType {
    fn from(val: u8) -> Self {
        let idx = val as usize;
        assert!(idx < TRY_NEXT_TABLE.len(), "malformed tryNextType:{}", idx);

        *TRY_NEXT_TABLE.get(idx).expect("malformed tryNextType")
    }
}

impl Default for TryNextType {
    fn default() -> Self {
        TryNextType::TryNext
    }
}
