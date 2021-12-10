use Operation::*;
pub const OPERATION_NUM: usize = 4;
#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum Operation {
    Get = 0u8,
    MGet,
    Store,
    Meta,
    Quit,
    Other,
}

impl Default for Operation {
    #[inline(always)]
    fn default() -> Self {
        Operation::Other
    }
}

const OPS: [Operation; 5] = [Get, MGet, Store, Meta, Other];
const OP_NAMES: [&'static str; OPS.len()] = ["get", "mget", "store", "meta", "other"];

impl From<usize> for Operation {
    #[inline(always)]
    fn from(op: usize) -> Self {
        debug_assert!(op < OPS.len());
        OPS[op]
    }
}

impl PartialEq for Operation {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        *self as u8 == *other as u8
    }
}
impl Eq for Operation {}
impl Operation {
    #[inline(always)]
    pub fn name(&self) -> &'static str {
        OP_NAMES[*self as u8 as usize]
    }
    #[inline(always)]
    pub fn is_retrival(&self) -> bool {
        *self as usize <= MGet as usize
    }
}
use std::hash::{Hash, Hasher};
impl Hash for Operation {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        (*self as u8).hash(state)
    }
}
