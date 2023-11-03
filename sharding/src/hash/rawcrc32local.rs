use super::{Crc32local, Hash};

// 用于支持groupchat中的UidSelectionStrategy
// 算法描述：1 优先解析“_”之前的uid，没有"_"解析整个字符串，如果是数字，直接返回；
//         2 否则进行crc32local计算
#[derive(Clone, Debug)]
pub struct Rawcrc32local {
    crc32local: Crc32local,
}

impl Default for Rawcrc32local {
    fn default() -> Self {
        Rawcrc32local {
            crc32local: Crc32local::default(),
        }
    }
}

const DELIMITER_UNDERSCORE: u8 = '_' as u8;

impl Hash for Rawcrc32local {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        let mut hash = 0;

        // 先尝试按uid_xxx来hash
        for i in 0..key.len() {
            let c = key.at(i);
            if !c.is_ascii_digit() {
                // 非数字必须是“_”，否则整体做crc32local计算
                if c == DELIMITER_UNDERSCORE {
                    return hash;
                } else {
                    hash = 0;
                    break;
                }
            }
            hash = hash.wrapping_mul(10) + (key.at(i) - '0' as u8) as i64;
        }

        // hash如果为0，进行crc32local
        if hash == 0 {
            return self.crc32local.hash(key);
        } else if hash > 0 {
            return hash;
        } else {
            log::error!("found malform rawcrc32local for key: {:?}", key);
            return 0;
        }
    }
}
