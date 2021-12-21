use std::sync::atomic::{
    AtomicBool, AtomicU32,
    Ordering::{self, *},
};
use std::sync::Arc;

use crate::{Id, ItemData};

pub(crate) trait ItemWriter {
    fn write(&mut self, name: &str, key: &str, sub_key: &str, val: f64);
}

use lazy_static::lazy_static;

lazy_static! {
    pub(crate) static ref EMPTY_ITEM: Arc<Item> = Default::default();
}

unsafe impl Send for Item {}
unsafe impl Sync for Item {}

pub struct ItemRc {
    pub(crate) inner: *const Item,
}
impl ItemRc {
    #[inline(always)]
    pub fn uninit() -> ItemRc {
        Self {
            inner: 0 as *const _,
        }
    }
    #[inline(always)]
    pub fn inited(&self) -> bool {
        !self.inner.is_null()
    }
    #[inline(always)]
    pub fn try_init(&mut self, idx: usize) {
        if let Some(item) = crate::get_metric(idx) {
            debug_assert!(!item.is_null());
            self.inner = item;
            debug_assert!(self.inited());
            self.incr_rc();
        }
    }
}
use std::ops::Deref;
impl Deref for ItemRc {
    type Target = Item;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        debug_assert!(self.inited());
        unsafe { &*self.inner }
    }
}
impl Drop for ItemRc {
    #[inline]
    fn drop(&mut self) {
        if self.inited() {
            self.decr_rc();
        }
    }
}

#[derive(Default, Debug)]
pub struct Item {
    lock: AtomicBool,
    pub(crate) rc: AtomicU32,
    data: ItemData,
}
impl Item {
    #[inline]
    pub(crate) fn init(&mut self, id: Arc<Id>) {
        assert_eq!(self.rc(), 0);
        debug_assert!(!id.t.is_empty());
        self.data.init_id(id);
        self.incr_rc();
    }
    #[inline(always)]
    pub(crate) fn inited(&self) -> bool {
        self.rc() > 0
    }
    #[inline(always)]
    pub(crate) fn data(&self) -> &ItemData {
        debug_assert!(self.inited());
        &self.data
    }

    #[inline(always)]
    pub(crate) fn snapshot<W: crate::ItemWriter>(&self, w: &mut W, secs: f64) {
        self.data().snapshot(w, secs);
    }
    #[inline]
    pub(crate) fn rc(&self) -> usize {
        self.rc.load(Ordering::Acquire) as usize
    }
    #[inline]
    fn incr_rc(&self) -> usize {
        self.rc.fetch_add(1, Ordering::AcqRel) as usize
    }
    #[inline]
    fn decr_rc(&self) -> usize {
        self.rc.fetch_sub(1, Ordering::AcqRel) as usize
    }
    // 没有任何引用，才能够获取其mut
    pub(crate) fn try_lock<'a>(&self) -> Option<ItemWriteGuard<'a>> {
        self.lock
            .compare_exchange(false, true, AcqRel, Relaxed)
            .map(|_| ItemWriteGuard {
                item: unsafe { &mut *(self as *const _ as *mut _) },
            })
            .ok()
    }
    #[inline]
    fn unlock(&self) {
        self.lock
            .compare_exchange(true, false, AcqRel, Relaxed)
            .expect("unlock failed");
    }
}

pub struct ItemWriteGuard<'a> {
    item: &'a mut Item,
}
impl<'a> Deref for ItemWriteGuard<'a> {
    type Target = Item;
    fn deref(&self) -> &Self::Target {
        &self.item
    }
}
use std::ops::DerefMut;
impl<'a> DerefMut for ItemWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.item
    }
}
impl<'a> Drop for ItemWriteGuard<'a> {
    fn drop(&mut self) {
        self.unlock();
    }
}
