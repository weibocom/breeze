impl<T: AsRef<[u8]>> Slicer for T {
    #[inline(always)]
    fn len(&self) -> usize {
        self.as_ref().len()
    }
    #[inline(always)]
    fn with_seg<R: Range, O: Merge>(&self, r: R, mut v: impl FnMut(&[u8], usize, bool) -> O) -> O {
        let (start, end) = r.range(self);
        debug_assert!(start <= end && end <= self.len() as usize);
        v(self.as_ref(), start, false)
    }
}
impl Slicer for str {
    #[inline(always)]
    fn len(&self) -> usize {
        self.len()
    }
    #[inline(always)]
    fn with_seg<R: Range, O: Merge>(&self, r: R, v: impl FnMut(&[u8], usize, bool) -> O) -> O {
        self.as_bytes().with_seg(r, v)
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
pub trait Slicer {
    fn len(&self) -> usize;
    fn with_seg<R: Range, O: Merge>(&self, r: R, v: impl FnMut(&[u8], usize, bool) -> O) -> O;
}
pub trait Merge {
    fn merge(self, other: impl FnMut() -> Self) -> Self;
}

pub trait Range {
    #[inline(always)]
    fn len<S: Slicer>(&self, s: &S) -> usize {
        self.r_len(s)
    }
    #[inline(always)]
    fn r_len<S: Slicer>(&self, s: &S) -> usize {
        let r = self.range(s);
        r.1 - r.0
    }
    fn range<S: Slicer>(&self, s: &S) -> (usize, usize);
    #[inline]
    fn start<S: Slicer>(&self, s: &S) -> usize {
        self.range(s).0
    }
}

pub trait Visit {
    fn check(&mut self, b: u8, idx: usize) -> bool;
}
impl Visit for u8 {
    #[inline(always)]
    fn check(&mut self, b: u8, _idx: usize) -> bool {
        *self == b
    }
}
impl<T: FnMut(u8, usize) -> bool> Visit for T {
    #[inline(always)]
    fn check(&mut self, b: u8, idx: usize) -> bool {
        self(b, idx)
    }
}

type Offset = usize;
impl Range for Offset {
    #[inline(always)]
    fn range<S: Slicer>(&self, s: &S) -> (usize, usize) {
        debug_assert!(*self <= s.len());
        (*self, s.len())
    }
}

impl Range for std::ops::Range<usize> {
    #[inline(always)]
    fn range<S: Slicer>(&self, s: &S) -> (usize, usize) {
        debug_assert!(self.start <= s.len());
        debug_assert!(self.end <= s.len());
        (self.start, self.end)
    }
}
impl Range for std::ops::RangeFrom<usize> {
    #[inline(always)]
    fn range<S: Slicer>(&self, s: &S) -> (usize, usize) {
        debug_assert!(self.start <= s.len());
        (self.start, s.len())
    }
}
impl Range for std::ops::RangeTo<usize> {
    #[inline(always)]
    fn range<S: Slicer>(&self, s: &S) -> (usize, usize) {
        debug_assert!(self.end <= s.len());
        (0, self.end)
    }
}
impl Range for std::ops::RangeFull {
    #[inline(always)]
    fn range<S: Slicer>(&self, s: &S) -> (usize, usize) {
        (0, s.len())
    }
}
