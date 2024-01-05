#[macro_use]
extern crate lazy_static;

mod id;
mod ip;
pub mod prometheus;
mod register;
mod types;

pub use crate::pub_status::Status;
pub use id::*;
pub use ip::*;
pub use register::*;
pub use types::*;

use std::fmt::Debug;
use std::ops::AddAssign;

// tests only
use crate::Snapshot;

use crate::ItemData;
use std::sync::Arc;

pub struct Metric {
    item: ItemPtr,
}
impl Metric {
    #[inline]
    pub(crate) fn from(item: ItemPtr) -> Self {
        Self { item }
    }
    // 检查metric是否已经注册到global
    pub fn check_registered(&mut self) -> bool {
        if self.item.is_local() {
            self.rsync(false);
        }
        self.item().is_global()
    }
    // 部分场景需要依赖Arc<Metric>，又需要对Metric进行+=的更新操作，因此需要将只读metric更新为mut
    // 1. 所有的基于metrics的操作都是原子的
    // 2. metric中的item一旦关联到global，则不再会更新。
    // 因此，使用这个方法必须确保Metric已经注册完成，即对应的item是global的。
    // 否则会触发item的更新。
    // 使用该方法前，必须要调用check_registered并且确保返回值为true, 否则UB
    #[inline(always)]
    pub unsafe fn as_mut(&self) -> &mut Self {
        assert!(self.item().is_global(), "{self:?}");
        #[allow(invalid_reference_casting)]
        &mut *(self as *const _ as *mut _)
    }
    #[inline(always)]
    fn item(&self) -> &Item {
        &*self.item
    }
    // 从global 同步item
    #[cold]
    #[inline(never)]
    fn rsync(&mut self, flush: bool) {
        debug_assert!(self.item().is_local());
        if let Position::Local(id) = &self.item().pos {
            if let Some(item) = crate::get_item(&*id) {
                self.item = ItemPtr::global(item);
            }
        } else {
            if flush {
                self.item().flush();
            }
        }
    }
}
impl<T: IncrTo + Debug> AddAssign<T> for Metric {
    #[inline(always)]
    fn add_assign(&mut self, m: T) {
        m.incr_to(self.item().data());
        if self.item().is_local() {
            self.rsync(true);
        }
    }
}
use std::ops::SubAssign;
impl<T: IncrTo + std::ops::Neg<Output = T> + Debug> SubAssign<T> for Metric {
    #[inline(always)]
    fn sub_assign(&mut self, m: T) {
        *self += -m;
    }
}
use std::fmt::{self, Display, Formatter};
impl Display for Metric {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.item().pos {
            Position::Global(idx) => crate::with_metric_id(*idx, |id| write!(f, "name:{id:?}")),
            Position::Local(id) => write!(f, "name:{:?}", id),
        }
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

pub(crate) trait WriteTo {
    fn write_to<W: ItemWriter>(&self, w: &mut W);
}

use ds::NumStr;
impl WriteTo for i64 {
    #[inline]
    fn write_to<W: ItemWriter>(&self, w: &mut W) {
        let v = if *self < 0 {
            w.put_slice(b"-");
            (*self * -1) as usize
        } else {
            *self as usize
        };
        v.with_str(|s| w.put_slice(s));
    }
}
impl WriteTo for f64 {
    #[inline]
    fn write_to<W: ItemWriter>(&self, w: &mut W) {
        let mut trunc = self.trunc() as i64;
        if *self < 0.0 {
            w.put_slice(b"-");
            trunc = -trunc;
        }
        (trunc as usize).with_str(|s| w.put_slice(s));
        let fraction = ((self.fract() * 1000.0) as i64).abs() as usize;
        if fraction > 0 {
            w.put_slice(b".");
            fraction.with_str(|s| w.put_slice(s));
        }
    }
}

pub(crate) trait ItemWriter {
    fn put_slice<S: AsRef<[u8]>>(&mut self, data: S);
    fn write<V: WriteTo>(&mut self, name: &str, key: &str, sub_key: &str, val: V);
    fn write_opts<V: WriteTo>(
        &mut self,
        name: &str,
        key: &str,
        sub_key: &str,
        val: V,
        opts: Vec<(&str, &str)>,
    );
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Position {
    Global(usize),  // 说明Item在Chunk中分配，全局共享
    Local(Arc<Id>), // 说明Item在Local中分配。
}

// 所有的Item都由Metrics创建并且释放
#[derive(Debug)]
#[repr(align(64))]
pub struct Item {
    // 使用Position，避免global与local更新时的race
    pub(crate) pos: Position,
    data: ItemData,
}
impl Item {
    pub(crate) fn global(idx: usize) -> Self {
        Self {
            pos: Position::Global(idx),
            data: ItemData::default(),
        }
    }
    #[inline(always)]
    pub(crate) fn is_global(&self) -> bool {
        !self.is_local()
    }
    #[inline(always)]
    pub(crate) fn is_local(&self) -> bool {
        match self.pos {
            Position::Global(_) => false,
            Position::Local(_) => true,
        }
    }
    #[inline(always)]
    pub(crate) fn id(&self) -> &Arc<Id> {
        match &self.pos {
            Position::Local(id) => &id,
            Position::Global(_) => panic!("id called in global item"),
        }
    }
    pub(crate) fn local(id: Arc<Id>) -> Self {
        Self {
            pos: Position::Local(id),
            data: ItemData::default(),
        }
    }
    #[inline]
    pub(crate) fn data(&self) -> &ItemData {
        &self.data
    }

    #[inline]
    pub(crate) fn snapshot<W: crate::ItemWriter>(&self, id: &Id, w: &mut W, secs: f64) {
        id.t.snapshot(&id.path, &id.key, &self.data, w, secs);
    }
    #[inline]
    pub(crate) fn flush(&self) {
        debug_assert!(self.is_local());
        crate::flush_item(self);
    }
}

impl Drop for Item {
    fn drop(&mut self) {
        assert!(self.is_local(), "{:?}", self.pos);
        self.flush();
    }
}

pub(crate) struct ItemPtr {
    ptr: *const Item,
}
impl Clone for ItemPtr {
    #[inline(always)]
    fn clone(&self) -> Self {
        panic!("ItemPtr should not be cloned");
    }
}
impl ItemPtr {
    #[inline(always)]
    fn global(global: *const Item) -> Self {
        debug_assert!(!global.is_null());
        debug_assert!(unsafe { &*global }.is_global());
        Self { ptr: global }
    }
    #[inline(always)]
    fn local(id: Arc<Id>) -> Self {
        let item = Item::local(id);
        Self {
            ptr: Box::into_raw(Box::new(item)),
        }
    }
}
// 实现Deref
impl std::ops::Deref for ItemPtr {
    type Target = Item;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        debug_assert!(!self.ptr.is_null());
        unsafe { &*self.ptr }
    }
}
// 实现Drop。如果是local，则释放内存
impl Drop for ItemPtr {
    #[inline(always)]
    fn drop(&mut self) {
        if self.is_local() {
            let raw = self.ptr as *mut Item;
            let _ = unsafe { Box::from_raw(raw) };
        }
    }
}
