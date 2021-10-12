mod read;
pub use read::*;

mod write;
pub use write::*;

pub fn cow<T>(t: T) -> (CowWriteHandle<T>, CowReadHandle<T>)
where
    T: Clone,
{
    let rx: CowReadHandle<T> = t.into();
    let tx = CowWriteHandle::from(rx.clone());
    (tx, rx)
}
