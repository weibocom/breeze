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

#[derive(Debug, Clone)]
pub enum TryNextType {
    NotTryNext = 0,
    TryNext = 1,
    Unkown = 2,
}

// (1) 0: not try next(对add/replace生效);  (2) 1: try next;  (3) 2:unkown (仅对set生效，注意提前考虑cas)
impl TryNextType {
    pub fn from(val: u8) -> Self {
        match val {
            0 => TryNextType::NotTryNext,
            1 => TryNextType::TryNext,
            2 => TryNextType::Unkown,
            _ => panic!("unknow try next type"),
        }
    }
}

impl Default for TryNextType {
    fn default() -> Self {
        TryNextType::TryNext
    }
}
