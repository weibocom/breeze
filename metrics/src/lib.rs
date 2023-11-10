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
use std::sync::Arc;

// tests only
pub use item::Item;

pub struct Metric {
    id: Arc<Id>,
    item: ItemRc,
}
impl Metric {
    #[inline]
    pub(crate) fn from(id: Arc<Id>) -> Self {
        let mut me = Self {
            id,
            item: ItemRc::uninit(),
        };
        me.try_inited();
        me
    }
    #[inline]
    fn try_inited(&mut self) {
        self.item.try_init(&self.id);
    }
    // 所有的基于metrics的操作都是原子的
    #[inline(always)]
    pub fn as_mut(&self) -> &mut Self {
        #[allow(invalid_reference_casting)]
        unsafe {
            &mut *(self as *const _ as *mut _)
        }
    }
    pub fn inited(&mut self) -> bool {
        self.item.inited()
    }
    // num类型，若未初始化则尝试初始化；若已经初始化，则值清0
    pub fn zero_num(&mut self) {
        if !self.inited() {
            self.try_inited();
        } else {
            self.item.data().zero_num();
        }
    }
}
impl<T: MetricData + Debug> AddAssign<T> for Metric {
    #[inline]
    fn add_assign(&mut self, m: T) {
        if self.item.inited() {
            m.incr_to(self.item.data());
        } else {
            m.incr_to_cache(&self.id);
            self.try_inited();
        }
    }
}
use std::ops::SubAssign;
impl<T: MetricData + std::ops::Neg<Output = T> + Debug> SubAssign<T> for Metric {
    #[inline]
    fn sub_assign(&mut self, m: T) {
        *self += -m;
    }
}
use std::fmt::{self, Display, Formatter};
impl Display for Metric {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "name:{:?}", self.id.path)
    }
}
impl Debug for Metric {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "name:{:?}", self.id)
    }
}
unsafe impl Sync for Metric {}
unsafe impl Send for Metric {}
