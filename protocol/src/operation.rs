use Operation::*;
pub const OPERATION_NUM: usize = 4;
#[repr(u8)]
#[derive(Copy, Clone, Debug)]
pub enum Operation {
    Get = 0u8,
    Gets,
    Store,
    Meta,
    Other,
}

impl Default for Operation {
    #[inline(always)]
    fn default() -> Self {
        Operation::Other
    }
}

impl From<usize> for Operation {
    #[inline(always)]
    fn from(op: usize) -> Self {
        match op {
            0 => Get,
            1 => Gets,
            2 => Store,
            3 => Meta,
            _ => Other,
        }
    }
}

impl PartialEq for Operation {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        *self as u8 == *other as u8
    }
}
impl Eq for Operation {}
const OP_NAMES: [&'static str; 5] = ["get", "mget", "store", "meta", "other"];
impl Operation {
    #[inline(always)]
    pub fn name(&self) -> &'static str {
        OP_NAMES[*self as u8 as usize]
    }
}
use std::hash::{Hash, Hasher};
impl Hash for Operation {
    #[inline]
    fn hash<H: Hasher>(&self, state: &mut H) {
        (*self as u8).hash(state)
    }
}
