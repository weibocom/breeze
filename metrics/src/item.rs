use super::KvItem;
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct SnapshotItem<E> {
    pub(crate) inner: Vec<HashMap<&'static str, E>>,
}

//boedn3cjnxkwqs
impl<E> SnapshotItem<E> {
    pub fn new() -> Self {
        Self {
            inner: Vec::with_capacity(32),
        }
    }
    #[inline]
    pub fn apply<V>(&mut self, pos: usize, key: &'static str, val: V)
    where
        E: AddAssign<V>,
        V: Into<E>,
    {
        for _ in self.inner.len()..=pos {
            self.inner.push(HashMap::with_capacity(16));
        }
        let ele = unsafe { self.inner.get_unchecked_mut(pos) };
        if let Some(e_val) = ele.get_mut(key) {
            *e_val += val;
        } else {
            let new = val.into();
            ele.insert(key, new);
        }
    }
    #[inline]
    pub(crate) fn take(&mut self) -> Self
    where
        E: KvItem,
    {
        let me = Self {
            inner: std::mem::take(&mut self.inner),
        };
        self.inner.reserve(me.inner.len());
        for i in 0..self.inner.len() {
            unsafe {
                self.inner
                    .push(HashMap::with_capacity(me.inner.get_unchecked(i).capacity()));
            }
        }
        me
    }
    #[inline]
    pub(crate) fn reset(&mut self)
    where
        E: KvItem,
    {
        if E::clear() {
            for map in self.inner.iter_mut() {
                map.clear();
            }
        }
    }
}

use std::ops::AddAssign;
impl<E> AddAssign for SnapshotItem<E>
where
    E: AddAssign,
{
    #[inline]
    fn add_assign(&mut self, other: Self) {
        for (i, group) in other.inner.into_iter().enumerate() {
            for (k, v) in group {
                self.apply(i, k, v);
            }
        }
    }
}

impl<E> Default for SnapshotItem<E> {
    fn default() -> Self {
        Self::new()
    }
}

use std::ops::Deref;
impl<E> Deref for SnapshotItem<E> {
    type Target = Vec<HashMap<&'static str, E>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
