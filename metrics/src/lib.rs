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

use crate::ItemData0;
use std::sync::{
    atomic::{AtomicBool, Ordering::*},
    Arc,
};

pub struct Metric {
    item: *const Item,
}
impl Metric {
    #[inline]
    pub(crate) fn from(item: *const Item) -> Self {
        Self { item }
    }
    //
    pub fn check_registered(&mut self) -> bool {
        let registered = self.check_get().is_global();
        log::info!("check_registered: {self} {registered}");
        registered
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
    pub(crate) fn item(&self) -> &Item {
        debug_assert!(!self.item.is_null());
        //println!("on_metric_drop:{:p}", self.inner);
        unsafe { &*self.item }
    }
    // 从global 同步item
    #[cold]
    #[inline(never)]
    fn rsync(&mut self) {
        assert!(self.item().is_local());
        if let Position::Local(id) = &self.item().pos {
            if let Some(item) = crate::get_item(&*id) {
                let local = self.item;
                self.item = item;
                unsafe { (&*local).detach() };
            }
        }
    }
    // get操作会触发更新
    #[inline(always)]
    pub(crate) fn check_get(&mut self) -> &Item {
        debug_assert!(!self.item.is_null());
        if self.item().is_local() {
            self.rsync();
        }
        self.item()
    }
    #[inline(always)]
    pub(crate) fn try_detach(&self) {
        let item = self.item();
        // 必须为local的，当前global的item不会释放
        if item.is_local() {
            item.detach();
        }
    }
}
impl<T: IncrTo + Debug> AddAssign<T> for Metric {
    #[inline(always)]
    fn add_assign(&mut self, m: T) {
        m.incr_to(self.item().data0());
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
impl Drop for Metric {
    #[inline]
    fn drop(&mut self) {
        self.try_detach();
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
    // 本地的Metric，并且对应的Metric已经drop掉。该Item可以被安全的回收。
    local_attached: AtomicBool,
    data: ItemData,
}
impl Item {
    pub(crate) fn global(idx: usize) -> Self {
        Self {
            pos: Position::Global(idx),
            data: ItemData::default(),
            local_attached: false.into(),
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
        assert!(self.is_local());
        match &self.pos {
            Position::Local(id) => &id,
            Position::Global(_) => panic!("id called in global item"),
        }
    }
    pub(crate) fn local(id: Arc<Id>) -> Self {
        Self {
            pos: Position::Local(id),
            data: ItemData::default(),
            local_attached: true.into(),
        }
    }
    #[inline]
    pub(crate) fn data0(&self) -> &ItemData0 {
        &self.data.inner
    }
    #[inline]
    pub(crate) fn is_attached(&self) -> bool {
        assert!(self.is_local());
        self.local_attached.load(Acquire)
    }
    #[inline]
    pub(crate) fn detach(&self) {
        log::info!("metric detached: {:?} => {:p}", self, self);
        self.local_attached
            .compare_exchange(true, false, AcqRel, Acquire)
            .expect("double attach");
    }

    #[inline]
    pub(crate) fn snapshot<W: crate::ItemWriter>(&self, id: &Id, w: &mut W, secs: f64) {
        id.t.snapshot(&id.path, &id.key, &self.data.inner, w, secs);
    }
}

impl Drop for Item {
    fn drop(&mut self) {
        log::info!("item drop:{:?}", self);
        // 表明对应的Metric是本地的，Item释放之前，需要将对应的Metric释放掉。
        assert!(self.is_local(), "{:?}", self.pos);
    }
}
