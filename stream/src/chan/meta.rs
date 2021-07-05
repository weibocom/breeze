use futures::ready;

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};
use protocol::{MetaType, Protocol};

pub struct MetaStream<P, B> {
    instances: Vec<B>,
    idx: usize,
    parser: P,
}

impl<P, B> MetaStream<P, B> {
    pub fn from(parser: P, instances: Vec<B>) -> Self {
        Self {
            idx: 0,
            instances: instances,
            parser: parser,
        }
    }
}

impl<P, B> AsyncReadAll for MetaStream<P, B>
where
    P: Unpin,
    B: AsyncReadAll + Unpin,
{
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Response>> {
        let me = &mut *self;
        unsafe { Pin::new(me.instances.get_unchecked_mut(me.idx)).poll_next(cx) }
    }
}

impl<P, B> AsyncWriteAll for MetaStream<P, B>
where
    P: Protocol,
    B: AsyncWriteAll + Unpin,
{
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        match me.parser.meta_type(buf.data()) {
            MetaType::Version => {
                // 只需要发送请求到一个backend即可
                for (i, b) in me.instances.iter_mut().enumerate() {
                    ready!(Pin::new(b).poll_write(cx, buf))?;
                    me.idx = i;
                    return Poll::Ready(Ok(()));
                }
            }
        }
        return Poll::Ready(Err(Error::new(
            ErrorKind::Other,
            "all meta instance failed",
        )));
    }
}
