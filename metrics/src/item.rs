use super::KvItem;
use std::collections::HashMap;

#[derive(Debug)]
pub(crate) struct SnapshotItem<E> {
    pub(crate) inner: HashMap<usize, HashMap<&'static str, E>>,
}

impl<E> SnapshotItem<E> {
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }
    #[inline]
    pub fn apply<V>(&mut self, service: usize, key: &'static str, val: V)
    where
        E: AddAssign<V>,
        V: Into<E>,
    {
        let ele = if let Some(g) = self.inner.get_mut(&service) {
            g
        } else {
            self.inner.insert(service, Default::default());
            self.inner.get_mut(&service).unwrap()
        };
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
        Self {
            inner: std::mem::take(&mut self.inner),
        }
    }
    #[inline]
    pub(crate) fn reset(&mut self)
    where
        E: KvItem,
    {
        if E::clear() {
            for (_, map) in self.inner.iter_mut() {
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
        for (i, group) in other.inner.into_iter() {
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
    type Target = HashMap<usize, HashMap<&'static str, E>>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
