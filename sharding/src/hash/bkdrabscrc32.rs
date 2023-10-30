use super::{bkdr::Bkdr, crc32::Crc32, Hash};

/// key：${num}#suffix，对数字部分进行 bkdr + abs + crc32；属小众业务算法。
/// 对于i32溢出，一般有类型提升 vs abs两种策略，bkdr自身是有abs，但考虑避免后面bkdr符号变化的影响，此处仍然进行abs；
#[derive(Debug, Clone)]
pub struct BkdrAbsCrc32 {
    bkdr: Bkdr,
    crc32: Crc32,
}

impl Default for BkdrAbsCrc32 {
    fn default() -> Self {
        Self {
            bkdr: Default::default(),
            crc32: Default::default(),
        }
    }
}

impl Hash for BkdrAbsCrc32 {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        // 定位到num结束的位置，拿到hashkey
        let mut hash_key = String::with_capacity(key.len());
        for i in 0..key.len() {
            let c = key.at(i);
            if c.is_ascii_digit() {
                hash_key.push(c as char);
            } else {
                // 遇到第一个非数字停止
                break;
            }
        }

        if hash_key.len() == 0 {
            log::warn!("found malformed bkdrabscrc32 key:{:?}", key);
            return 0;
        }

        // bkdr
        let hash = self.bkdr.hash(&hash_key.as_bytes());
        // abs + string
        let hash_abs = hash.abs().to_string();
        // crc32
        self.crc32.hash(&hash_abs.as_bytes())
    }
}
