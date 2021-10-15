use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};

pub enum AsyncOperation<Get, MGet, Store, Meta> {
    Get(Get),
    MGet(MGet),
    Store(Store),
    Meta(Meta),
}

impl<Get, MGet, Store, Meta> AsyncReadAll for AsyncOperation<Get, MGet, Store, Meta>
where
    Get: AsyncReadAll + Unpin,
    MGet: AsyncReadAll + Unpin,
    Store: AsyncReadAll + Unpin,
    Meta: AsyncReadAll + Unpin,
{
    #[inline(always)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        match me {
            Self::Get(ref mut s) => Pin::new(s).poll_next(cx),
            Self::MGet(ref mut s) => Pin::new(s).poll_next(cx),
            Self::Store(ref mut s) => Pin::new(s).poll_next(cx),
            Self::Meta(ref mut s) => Pin::new(s).poll_next(cx),
        }
    }
}
impl<Get, MGet, Store, Meta> AsyncWriteAll for AsyncOperation<Get, MGet, Store, Meta>
where
    Get: AsyncWriteAll + Unpin,
    MGet: AsyncWriteAll + Unpin,
    Store: AsyncWriteAll + Unpin,
    Meta: AsyncWriteAll + Unpin,
{
    #[inline(always)]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        match me {
            Self::Get(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Self::MGet(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Self::Store(ref mut s) => Pin::new(s).poll_write(cx, buf),
            Self::Meta(ref mut s) => Pin::new(s).poll_write(cx, buf),
        }
    }
}
use crate::{Address, Addressed};
impl<Get, MGet, Store, Meta> Addressed for AsyncOperation<Get, MGet, Store, Meta>
where
    Get: Addressed + Unpin,
    MGet: Addressed + Unpin,
    Store: Addressed + Unpin,
    Meta: Addressed + Unpin,
{
    #[inline(always)]
    fn addr(&self) -> Address {
        match self {
            Self::Get(s) => s.addr(),
            Self::MGet(s) => s.addr(),
            Self::Store(s) => s.addr(),
            Self::Meta(s) => s.addr(),
        }
    }
}
