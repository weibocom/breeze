use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};

use byteorder::{ByteOrder, LittleEndian};
use crypto::digest::Digest;
use crypto::md5::Md5;

use crate::backend::AddressEnable;
use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};
use hash::Hash;
use protocol::Protocol;

pub struct AsyncSharding<B, H, P> {
    idx: usize,
    // 用于modula分布
    shards: Vec<B>,
    // 用于一致性hash分布
    consistent_enable: bool,
    consistent_map: BTreeMap<u64, usize>,
    hasher: H,
    parser: P,
}

impl<B, H, P> AsyncSharding<B, H, P>
where
    B: AddressEnable,
{
    pub fn from(shards: Vec<B>, hasher: H, consistent_enable: bool, parser: P) -> Self {
        // 构建一致性hash的treemap，对java版一致性hash做了简化，不支持权重  fishermen
        let mut consistent_map = BTreeMap::new();
        for idx in 0..shards.len() {
            let factor = 40;
            for i in 0..factor {
                let mut md5 = Md5::new();
                let data: String = shards[idx].get_address() + "-" + &i.to_string();
                let data_str = data.as_str();
                md5.input_str(data_str);
                let digest_str = md5.result_str();
                let digest_bytes = digest_str.as_bytes();
                for j in 0..3 {
                    let hash = LittleEndian::read_i32(&digest_bytes[j * 4..]).wrapping_abs() as u64;
                    consistent_map.insert(hash, idx);
                }
            }
        }

        let idx = 0;
        Self {
            idx,
            shards,
            consistent_enable,
            consistent_map,
            hasher,
            parser,
        }
    }
}

impl<B, H, P> AsyncWriteAll for AsyncSharding<B, H, P>
where
    B: AsyncWriteAll + Unpin,
    H: Unpin + Hash,
    P: Unpin + Protocol,
{
    #[inline]
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context, buf: &Request) -> Poll<Result<()>> {
        let me = &mut *self;
        debug_assert!(me.idx < me.shards.len());
        let key = me.parser.key(buf.data());
        let h = me.hasher.hash(key) as usize;
        // 根据分布策略确定idx
        if !me.consistent_enable {
            me.idx = h % me.shards.len();
        } else {
            me.consistent_map.
        }
        

        unsafe { Pin::new(me.shards.get_unchecked_mut(me.idx)).poll_write(cx, buf) }
    }
}

impl<B, H, P> AsyncReadAll for AsyncSharding<B, H, P>
where
    B: AsyncReadAll + Unpin,
    H: Unpin,
    P: Unpin,
{
    #[inline]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Response>> {
        let me = &mut *self;
        if me.shards.len() == 0 {
            return Poll::Ready(Err(Error::new(
                ErrorKind::NotConnected,
                "not connected, maybe topology not inited",
            )));
        }
        unsafe { Pin::new(me.shards.get_unchecked_mut(me.idx)).poll_next(cx) }
    }
}
