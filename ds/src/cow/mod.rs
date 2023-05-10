mod read;
pub use read::*;

mod write;
pub use write::*;

pub fn cow<T: Clone>(t: T) -> (CowWriteHandle<T>, CowReadHandle<T>) {
    let rx: CowReadHandle<T> = t.into();
    let tx = CowWriteHandle::from(rx.clone());
    (tx, rx)
}
