#[macro_use]
extern crate lazy_static;

mod id;
mod ip;
mod item;
mod macros;
mod packet;
pub mod prometheus;
mod register;
mod sender;
mod types;

pub use id::*;
pub use ip::*;
use item::*;
pub use macros::*;
pub use register::*;
pub use sender::*;
pub use types::*;

use crate::{Id, ItemRc};
use std::fmt::Debug;
use std::ops::AddAssign;

// tests only
pub use item::Item;

pub struct Metric {
    item: ItemRc,
}
impl Metric {
    #[inline]
    pub(crate) fn from(item: &Item) -> Self {
        let item = ItemRc {
            inner: item as *const _,
        };
        Self { item }
    }
    // 所有的基于metrics的操作都是原子的
    #[inline(always)]
    pub fn as_mut(&self) -> &mut Self {
        #[allow(invalid_reference_casting)]
        unsafe {
            &mut *(self as *const _ as *mut _)
        }
    }
    pub fn id(&self) -> &str {
        "not impl"
    }
}
impl<T: IncrTo + Debug> AddAssign<T> for Metric {
    #[inline]
    fn add_assign(&mut self, m: T) {
        log::info!("add_assign:{:?}", m);
        m.incr_to(&self.item.get().data0());
        log::info!("add_assign complete :{:?}", m);
    }
}
use std::ops::SubAssign;
impl<T: IncrTo + std::ops::Neg<Output = T> + Debug> SubAssign<T> for Metric {
    #[inline]
    fn sub_assign(&mut self, m: T) {
        *self += -m;
    }
}
use std::fmt::{self, Display, Formatter};
impl Display for Metric {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "name:{:?}", self.id())
    }
}
impl Debug for Metric {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
unsafe impl Sync for Metric {}
unsafe impl Send for Metric {}
