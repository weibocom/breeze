use super::RingBufferStream;

use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::io::ReadBuf;

pub trait IdAsyncRead {
    fn poll_read(&self, id: usize, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>>;
}
pub trait IdAsyncWrite {
    fn poll_write(&self, id: usize, cx: &mut Context, buf: &[u8]) -> Poll<Result<()>>;
    fn poll_shutdown(&self, id: usize, cx: &mut Context) -> Poll<Result<()>>;
}

pub enum IdStream {
    Mpsc(Arc<RingBufferStream>),
    NotConnected(NotConnected),
}

impl IdStream {
    pub fn not_connected() -> Self {
        IdStream::NotConnected(NotConnected)
    }
}

impl IdAsyncRead for IdStream {
    fn poll_read(&self, id: usize, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
        match self {
            IdStream::Mpsc(stream) => stream.poll_read(id, cx, buf),
            // TODO 记录一次异常
            IdStream::NotConnected(stream) => stream.poll_read(id, cx, buf),
        }
    }
}

impl IdAsyncWrite for IdStream {
    fn poll_write(&self, id: usize, cx: &mut Context, buf: &[u8]) -> Poll<Result<()>> {
        match self {
            IdStream::Mpsc(stream) => stream.poll_write(id, cx, buf),
            IdStream::NotConnected(stream) => stream.poll_write(id, cx, buf),
        }
    }
    fn poll_shutdown(&self, id: usize, cx: &mut Context) -> Poll<Result<()>> {
        match self {
            IdStream::Mpsc(stream) => stream.poll_shutdown(id, cx),
            IdStream::NotConnected(stream) => stream.poll_shutdown(id, cx),
        }
    }
}

pub struct NotConnected;

impl NotConnected {
    pub fn new() -> Self {
        NotConnected
    }
}
impl IdAsyncRead for NotConnected {
    fn poll_read(&self, _id: usize, _cx: &mut Context, _buf: &mut ReadBuf) -> Poll<Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl IdAsyncWrite for NotConnected {
    fn poll_write(&self, _id: usize, _cx: &mut Context, _buf: &[u8]) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "writing to a not connected id-stream",
        )))
    }
    fn poll_shutdown(&self, _id: usize, _cx: &mut Context) -> Poll<Result<()>> {
        Poll::Ready(Err(Error::new(
            ErrorKind::NotConnected,
            "shutdown a not connected id-stream",
        )))
    }
}

pub trait Id {
    fn id(&self) -> usize;
}

impl Id for Cid {
    fn id(&self) -> usize {
        self.id
    }
}

pub struct Cid {
    id: usize,
    ids: Arc<Ids>,
}

impl Cid {
    pub fn new(id: usize, ids: Arc<Ids>) -> Self {
        Cid { id, ids }
    }
}

impl Drop for Cid {
    fn drop(&mut self) {
        self.ids.release(self.id);
    }
}

use std::sync::atomic::{AtomicBool, Ordering};
pub struct Ids {
    bits: Vec<AtomicBool>,
}

impl Ids {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            bits: (0..cap).map(|_| AtomicBool::new(false)).collect(),
        }
    }
    pub fn next(&self) -> Option<usize> {
        for (id, status) in self.bits.iter().enumerate() {
            match status.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => {
                    return Some(id);
                }
                Err(_) => {}
            }
        }
        None
    }

    pub fn release(&self, id: usize) {
        unsafe {
            match self.bits.get_unchecked(id).compare_exchange(
                true,
                false,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {}
                Err(_) => panic!("not a valid status."),
            }
        }
    }
}
