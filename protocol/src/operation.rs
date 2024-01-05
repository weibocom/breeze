use Operation::*;
pub const OPERATION_NUM: usize = 4;
#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum Operation {
    Get = 0u8,
    MGet,
    Gets,
    Store,
    Meta,
    Other,
}

pub type OpCode = u16;

impl Default for Operation {
    #[inline]
    fn default() -> Self {
        Operation::Other
    }
}

pub const OPS: [Operation; 6] = [Get, MGet, Gets, Store, Meta, Other];
const OP_NAMES: [&'static str; OPS.len()] = ["get", "mget", "gets", "store", "meta", "other"];

impl From<u8> for Operation {
    #[inline]
    fn from(op_idx: u8) -> Self {
        assert!((op_idx as usize) < OPS.len(), "op_idx: {}", op_idx);
        OPS[op_idx as usize]
    }
}

impl PartialEq for Operation {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        *self as u8 == *other as u8
    }
}
impl Eq for Operation {}
impl Operation {
    #[inline]
    pub fn name(&self) -> &'static str {
        OP_NAMES[*self as u8 as usize]
    }
    #[inline]
    pub fn master_only(&self) -> bool {
        *self as usize == Gets as usize || *self as usize == Meta as usize
    }
    #[inline]
    pub fn is_retrival(&self) -> bool {
        *self as usize <= Gets as usize
    }
    #[inline]
    pub fn id(&self) -> usize {
        *self as usize
    }
    #[inline]
    pub fn is_store(&self) -> bool {
        *self as usize == Store as usize
    }
    #[inline]
    pub fn is_cas(&self) -> bool {
        *self as usize == Gets as usize
    }
    #[inline]
    pub fn is_meta(&self) -> bool {
        *self as usize == Meta as usize
    }
    #[inline]
    pub fn is_query(&self) -> bool {
        *self == Get || *self == Gets || *self == MGet
    }
}
use std::hash::{Hash, Hasher};
impl Hash for Operation {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        (*self as u8).hash(state)
    }
}
