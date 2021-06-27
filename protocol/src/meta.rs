use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::chan::AsyncWriteAll;
use crate::memcache::MemcacheMetaStream;
macro_rules! define_meta_stream {
    ($($item:ident, $type_name:tt);+) => {

        pub enum MetaStream {
            $($item($type_name)),+
        }

        impl AsyncWriteAll for MetaStream{}

        impl AsyncRead for MetaStream {
            fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context, buf: &mut ReadBuf) -> Poll<Result<()>> {
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_read(cx, buf),)+
                }
            }
        }

        impl AsyncWrite for MetaStream {
            fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>>{
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_write(cx, buf),)+
                }
            }
            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_flush(cx),)+
                }
            }
            fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<()>> {
                match &mut *self {
                    $(Self::$item(ref mut p) => Pin::new(p).poll_shutdown(cx),)+
                }
            }
        }
    };
}
define_meta_stream! {
    Mc, MemcacheMetaStream
}
