use crypto::digest::Digest;
use crypto::md5::Md5;

use std::collections::BTreeMap;
use std::ops::Bound::Included;

#[derive(Clone)]
pub struct Consistent {
    buckets: BTreeMap<i64, usize>,
}

impl Consistent {
    pub fn index(&self, hash: u64) -> usize {
        let hash = hash as i64;
        // 从[hash, max)范围从map中寻找节点
        let idxs = self.buckets.range((Included(hash), Included(i64::MAX)));
        for (_h, idx) in idxs {
            return *idx;
        }

        // 如果idxs为空，则选择第一个hash节点,first_entry暂时是unstable，延迟使用
        for (_h, i) in self.buckets.iter() {
            return *i;
        }

        return 0;
    }

    pub fn from(shards: &Vec<String>) -> Self {
        let mut map = BTreeMap::default();
        for idx in 0..shards.len() {
            let factor = 40;
            for i in 0..factor {
                let mut md5 = Md5::new();
                let data: String = shards[idx].to_string() + "-" + &i.to_string();
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

                    map.insert(hash, idx);
                }
            }
        }
        Self { buckets: map }
    }
}
