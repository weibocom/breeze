use std::sync::atomic::{
    AtomicBool, AtomicU32,
    Ordering::{self, *},
};
use std::sync::Arc;

use crate::{Id, ItemData};

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

pub struct ItemRc {
    pub(crate) inner: *const Item,
}
impl ItemRc {
    #[inline]
    pub fn uninit() -> ItemRc {
        Self {
            inner: 0 as *const _,
        }
    }
    #[inline]
    pub fn inited(&self) -> bool {
        !self.inner.is_null()
    }
    #[inline]
    pub fn try_init(&mut self, id: &Arc<Id>) {
        if let Some(item) = crate::get_metric(id) {
            assert!(!item.is_null());
            self.inner = item;
            assert!(self.inited());
            self.incr_rc();
        }
    }
}
use std::ops::Deref;
impl Deref for ItemRc {
    type Target = Item;
    #[inline]
    fn deref(&self) -> &Self::Target {
        assert!(self.inited());
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
        assert!(!id.t.is_empty());
        self.data.init_id(id);
        self.incr_rc();
    }
    #[inline]
    pub(crate) fn inited(&self) -> bool {
        self.rc() > 0
    }
    #[inline]
    pub(crate) fn data(&self) -> &ItemData {
        assert!(self.inited());
        &self.data
    }

    #[inline]
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
                #[allow(invalid_reference_casting)]
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
