use std::sync::atomic::{AtomicUsize, Ordering, Ordering::*};
use std::sync::Arc;

use crossbeam_channel::{
    bounded as c_bounded, Receiver as CReceiver, Sender as CSender, TryRecvError, TrySendError,
};

pub fn bounded<T>(cap: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = c_bounded(cap);
    let num = Arc::new(AtomicUsize::new(0));
    (
        Sender {
            num: num.clone(),
            tx,
            _mark: Default::default(),
        },
        Receiver {
            num,
            rx,
            _mark: Default::default(),
        },
    )
}

pub struct Receiver<T> {
    num: Arc<AtomicUsize>,
    rx: CReceiver<T>,
    _mark: std::marker::PhantomData<T>,
}
pub struct Sender<T> {
    num: Arc<AtomicUsize>,
    tx: CSender<T>,
    _mark: std::marker::PhantomData<T>,
}

impl<T> Sender<T> {
    #[inline(always)]
    pub fn try_send(&self, t: T) -> Result<(), TrySendError<T>> {
        self.tx.try_send(t).map(|_| {
            self.num.fetch_add(1, AcqRel);
        })
    }
}

impl<T> Receiver<T> {
    #[inline(always)]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if self.num.load(Ordering::Acquire) > 0 {
            self.rx.try_recv().map(|t| {
                self.num.fetch_sub(1, AcqRel);
                t
            })
        } else {
            Err(TryRecvError::Empty)
        }
    }
}
