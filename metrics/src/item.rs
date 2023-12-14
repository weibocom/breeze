use std::sync::{
    atomic::{AtomicBool, Ordering::*},
    Arc,
};

use crate::{Id, ItemData, ItemData0};

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
    #[inline(always)]
    pub(crate) fn as_ref(&self) -> &Item {
        debug_assert!(!self.inner.is_null());
        unsafe { &*self.inner }
    }
    #[inline(always)]
    pub(crate) fn get(&mut self) -> &Item {
        debug_assert!(!self.inner.is_null());
        let inner = unsafe { &*self.inner };
        match &inner.pos {
            Position::Global(_) => inner,
            Position::Local(id) => {
                crate::get_item(&*id).map(|item| self.inner = item);
                unsafe { &*self.inner }
            }
        }
    }
    #[inline(always)]
    pub(crate) fn on_metric_drop(&self) {
        let item = self.as_ref();
        // 必须为local的，当前global的item不会释放
        assert!(item.is_local());
        item.local_dropped
            .compare_exchange(false, true, AcqRel, Relaxed)
            .expect("race");
    }
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
    local_dropped: AtomicBool,
    data: ItemData,
}
impl Item {
    pub(crate) fn global(idx: usize) -> Self {
        Self {
            pos: Position::Global(idx),
            data: ItemData::default(),
            local_dropped: false.into(),
        }
    }
    #[inline(always)]
    pub(crate) fn is_local(&self) -> bool {
        match self.pos {
            Position::Global(_) => false,
            Position::Local(_) => true,
        }
    }
    pub(crate) fn local(id: Arc<Id>) -> Self {
        Self {
            pos: Position::Local(id),
            data: ItemData::default(),
            local_dropped: false.into(),
        }
    }
    #[inline]
    pub(crate) fn data0(&self) -> &ItemData0 {
        &self.data.inner
    }

    #[inline]
    pub(crate) fn snapshot<W: crate::ItemWriter>(&self, id: &Id, w: &mut W, secs: f64) {
        use crate::Snapshot;
        id.t.snapshot(&id.path, &id.key, &self.data.inner, w, secs);
    }
}

impl Drop for Item {
    fn drop(&mut self) {
        if self.is_local() {
            // 表明对应的Metric是本地的，Item释放之前，需要将对应的Metric释放掉。
            assert!(self.local_dropped.load(Acquire));
        }
    }
}
