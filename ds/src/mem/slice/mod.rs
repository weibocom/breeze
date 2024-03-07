use crate::{Merge, Range as Ranger, Slicer};
use std::ops::Deref;
impl<T: Deref<Target = [u8]>> Slicer for T {
    #[inline(always)]
    fn len(&self) -> usize {
        self.deref().len()
    }
    #[inline(always)]
    fn with_seg<R: Ranger, O: Merge>(&self, r: R, mut v: impl FnMut(&[u8], usize, bool) -> O) -> O {
        let (start, end) = r.range(self);
        debug_assert!(start <= end && end <= self.len() as usize);
        v(&*self, start, false)
    }
}

impl<T> Merge for Option<T> {
    #[inline(always)]
    fn merge(self, other: impl FnMut() -> Self) -> Self {
        self.or_else(other)
    }
}
impl Merge for bool {
    #[inline(always)]
    fn merge(self, mut other: impl FnMut() -> Self) -> Self {
        self || other()
    }
}
impl Merge for () {
    #[inline(always)]
    fn merge(self, mut other: impl FnMut() -> Self) -> Self {
        other()
    }
}
impl<E> Merge for Result<(), E> {
    #[inline(always)]
    fn merge(self, mut other: impl FnMut() -> Self) -> Self {
        self.and_then(|_| other())
    }
}
