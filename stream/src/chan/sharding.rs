use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::ops::Bound::Included;
use std::pin::Pin;
use std::task::{Context, Poll};

use crypto::digest::Digest;
use crypto::md5::Md5;

use crate::backend::AddressEnable;
use crate::{AsyncReadAll, AsyncWriteAll, Request, Response};
use hash::Hash;
use protocol::Protocol;

pub struct AsyncSharding<B, H, P> {
    idx: usize,
    // 用于modula分布以及一致性hash定位server
    shards: Vec<B>,
    // 用于一致性hash分布
    consistent_enable: bool,
    consistent_map: BTreeMap<i64, usize>,
    hasher: H,
    parser: P,
}

impl<B, H, P> AsyncSharding<B, H, P>
where
    B: AddressEnable,
{
    pub fn from(shards: Vec<B>, hasher: H, distribution: &String, parser: P) -> Self {
        // 构建一致性hash的treemap，对java版一致性hash做了简化，不支持权重  fishermen
        let mut consistent_map = BTreeMap::new();
        for idx in 0..shards.len() {
            let factor = 40;
            for i in 0..factor {
                let mut md5 = Md5::new();
                let data: String = shards[idx].get_address() + "-" + &i.to_string();
                let data_str = data.as_str();
                md5.input_str(data_str);
                let mut out_bytes = [0u8; 16];
                md5.result(&mut out_bytes);
                for j in 0..4 {
                    let hash = (((out_bytes[3 + j * 4] & 0xFF) as i64) << 24)
                        | (((out_bytes[2 + j * 4] & 0xFF) as i64) << 16)
                        | (((out_bytes[1 + j * 4] & 0xFF) as i64) << 8)
                        | ((out_bytes[0 + j * 4] & 0xFF) as i64);

                    let mut hash = hash.wrapping_rem(i32::MAX as i64);
                    if hash < 0 {
                        hash = hash.wrapping_mul(-1);
                    }

                    consistent_map.insert(hash, idx);
                }
            }
        }

        let idx = 0;
        let consistent_enable = hash::DISTRIBUTION_CONSISTENT.eq(distribution.as_str());
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
            // 一致性hash，选择hash环的第一个节点，不支持漂移，避免脏数据 fishermen
            let (_, idx) = get_consistent_hash_idx(&me.consistent_map, h as i64);
            me.idx = idx;
            println!("use consistent hash idx/{}", idx);
        }

        unsafe { Pin::new(me.shards.get_unchecked_mut(me.idx)).poll_write(cx, buf) }
    }
}

fn get_consistent_hash_idx(consistent_map: &BTreeMap<i64, usize>, hash: i64) -> (i64, usize) {
    // 从[hash, max)范围从map中寻找节点
    let idxs = consistent_map.range((Included(hash), Included(i64::MAX)));
    for (h, idx) in idxs {
        return (*h, *idx);
    }

    // 如果idxs为空，则选择第一个hash节点,first_entry暂时是unstable，延迟使用
    //if let Some(mut entry) = self.consistent_map.first_entry() {
    for (h, i) in consistent_map {
        return (*h, *i);
    }

    debug_assert!(false);
    println!("++++ should not come here!!!");
    return (0, 0);
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
