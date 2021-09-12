/// 提供一写多读的高性能的数据结构。

pub trait Update<O> {
    fn update(&mut self, o: &mut O);
}

#[derive(Clone)]
pub struct Cow<T, O> {
    inner: T,
    _marker: std::marker::PhantomData<O>,
}

impl<T, O> std::ops::Deref for Cow<T, O> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub fn cow<T, O>(t: T) -> (CowWriteHandle<Cow<T, O>, O>, CowReadHandle<Cow<T, O>>)
where
    T: Clone + Update<O>,
    O: Clone,
{
    let t = Cow::from(t);
    let (tx, rx) = left_right::new_from_empty(t);
    (CowWriteHandle { inner: tx }, CowReadHandle { inner: rx })
}

impl<T, O> Cow<T, O> {
    pub fn from(t: T) -> Self {
        Self {
            inner: t,
            _marker: Default::default(),
        }
    }
}

use left_right::Absorb;
impl<T, O> Absorb<O> for Cow<T, O>
where
    T: Clone + Update<O>,
{
    fn absorb_first(&mut self, operation: &mut O, _other: &Self) {
        self.inner.update(operation);
    }
    fn sync_with(&mut self, first: &Self) {
        self.inner = first.inner.clone();
    }
}

#[derive(Clone)]
pub struct CowReadHandle<T> {
    inner: left_right::ReadHandle<T>,
}
impl<T> CowReadHandle<T> {
    pub fn read<F: Fn(&T) -> R, R>(&self, f: F) -> R {
        f(&*self.inner.enter().expect("not init-ed yes"))
    }
}
pub struct CowWriteHandle<T, O>
where
    T: left_right::Absorb<O>,
{
    inner: left_right::WriteHandle<T, O>,
}
impl<T, O> CowWriteHandle<T, O>
where
    T: left_right::Absorb<O>,
{
    pub fn write(&mut self, o: O) {
        self.inner.append(o);
        self.inner.publish();
    }
}
