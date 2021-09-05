use std::collections::HashMap;
pub(crate) struct SnapshotItem<E> {
    pub(crate) inner: Vec<HashMap<&'static str, E>>,
}

impl<E> SnapshotItem<E> {
    pub fn new() -> Self {
        Self {
            inner: Vec::with_capacity(32),
        }
    }
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
}

use std::ops::AddAssign;
impl<E> AddAssign for SnapshotItem<E>
where
    E: AddAssign,
{
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
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
