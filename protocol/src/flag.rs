use crate::{HashedCommand, OpCode, Operation};
pub type FlagExt = u64;
#[derive(Debug, Default)]
pub struct Flag {
    op_code: OpCode,
    op: Operation,
    //try_next_type: TryNextType,
    sentonly: bool,
    //status_ok: bool,
    noforward: bool,
    //nil_converted: bool, //是否进行了nil转换，用于设置req的rsp是否进行了nil convert【部分multi请求需要】
    v: FlagExt,
}

//impl std::ops::Deref for Flag {
//    type Target = u64;
//    #[inline]
//    fn deref(&self) -> &Self::Target {
//        &self.v
//    }
//}
//impl std::ops::DerefMut for Flag {
//    #[inline]
//    fn deref_mut(&mut self) -> &mut Self::Target {
//        &mut self.v
//    }
//}

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
impl Ext for FlagExt {
    #[inline]
    fn ext(&self) -> Self {
        *self
    }
    #[inline]
    fn ext_mut(&mut self) -> &mut Self {
        self
    }
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

    //#[inline]
    //pub fn set_try_next_type(&mut self, try_type: TryNextType) {
    //    self.try_next_type = try_type
    //}

    //#[inline]
    //pub fn try_next_type(&self) -> TryNextType {
    //    self.try_next_type.clone()
    //}

    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
    //#[inline]
    //pub fn set_status_ok(&mut self, ok: bool) -> &mut Self {
    //    debug_assert_eq!(self.ok(), false);
    //    self.status_ok = ok;
    //    self
    //}
    //#[inline]
    //pub fn ok(&self) -> bool {
    //    self.status_ok
    //}
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
    //#[inline]
    //pub fn set_nil_convert(&mut self) -> &mut Self {
    //    self.nil_converted = true;
    //    self
    //}
    //#[inline]
    //pub fn nil_converted(&self) -> bool {
    //    self.nil_converted
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

pub trait Ext {
    fn ext(&self) -> u64;
    fn ext_mut(&mut self) -> &mut u64;
}

pub trait Bit {
    fn mask_set(&mut self, shift: u8, mask: u64, val: u64);
    fn mask_get(&self, shift: u8, mask: u64) -> u64;
    fn set(&mut self, shift: u8);
    fn clear(&mut self, shift: u8);
    fn get(&self, shift: u8) -> bool;
}

impl<T: Ext> Bit for T {
    //mask决定val中要set的位数
    #[inline]
    fn mask_set(&mut self, shift: u8, mask: u64, val: u64) {
        debug_assert!(val <= mask);
        debug_assert!(shift as u32 + mask.trailing_ones() <= FlagExt::BITS);
        debug_assert_eq!(self.mask_get(shift, mask), 0);
        *self.ext_mut() |= val << shift;
        debug_assert_eq!(val, self.mask_get(shift, mask));
    }
    #[inline]
    fn mask_get(&self, shift: u8, mask: u64) -> u64 {
        debug_assert!(shift as u32 + mask.trailing_ones() <= FlagExt::BITS);
        (self.ext() >> shift) & mask
    }
    #[inline]
    fn set(&mut self, shift: u8) {
        debug_assert!(shift as u32 <= FlagExt::BITS);
        *self.ext_mut() |= 1 << shift;
    }
    #[inline]
    fn clear(&mut self, shift: u8) {
        debug_assert!(shift as u32 <= FlagExt::BITS);
        *self.ext_mut() &= !(1 << (shift));
    }
    #[inline]
    fn get(&self, shift: u8) -> bool {
        debug_assert!(shift as u32 <= FlagExt::BITS);
        self.ext() & (1 << shift) != 0
    }
}
