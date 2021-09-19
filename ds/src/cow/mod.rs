mod read;
pub use read::*;

mod write;
pub use write::*;

pub trait Update<O> {
    fn update(&mut self, op: &O);
}
pub fn cow<T, O>(t: T) -> (CowWriteHandle<T, O>, CowReadHandle<T>)
where
    T: Update<O> + Clone,
{
    let rx: CowReadHandle<T> = t.into();
    let tx = CowWriteHandle::from(rx.clone());
    (tx, rx)
}
