use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Response};

pub enum AsyncOperation<Get, Gets, Store, Meta> {
    Get(Get),
    Gets(Gets),
    Store(Store),
    Meta(Meta),
}

impl<Get, Gets, Store, Meta> AsyncReadAll for AsyncOperation<Get, Gets, Store, Meta>
where
    Get: AsyncReadAll + Unpin,
    Gets: AsyncReadAll + Unpin,
    Store: AsyncReadAll + Unpin,
    Meta: AsyncReadAll + Unpin,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        match me {
            Self::Get(ref mut s) => Pin::new(s).poll_next(cx),
            Self::Gets(ref mut s) => Pin::new(s).poll_next(cx),
            Self::Store(ref mut s) => Pin::new(s).poll_next(cx),
            Self::Meta(ref mut s) => Pin::new(s).poll_next(cx),
        }
    }
}
impl<Get, Gets, Store, Meta> AsyncWriteAll for AsyncOperation<Get, Gets, Store, Meta>
where
    Get: AsyncWriteAll + Unpin,
    Gets: AsyncWriteAll + Unpin,
    Store: AsyncWriteAll + Unpin,
    Meta: AsyncWriteAll + Unpin,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            Self::Get(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Self::Gets(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Self::Store(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Self::Meta(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }
}
