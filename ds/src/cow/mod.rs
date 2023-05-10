mod read;
pub use read::*;

mod write;
pub use write::*;

pub fn cow<T: Clone + Send + Sync>(t: T) -> (CowWriteHandle<T>, CowReadHandle<T>) {
    let rx: CowReadHandle<T> = t.into();
    let tx = CowWriteHandle::from(rx.clone());
    (tx, rx)
}
