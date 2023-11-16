// 对分隔符后缀之后的部分进行long型转换，当前按业务要求，强制分隔符个数为1 fishermen

use super::DebugName;
use std::fmt::Display;

#[derive(Clone, Default, Debug)]
pub struct RawSuffix {
    delimiter: u8,
    name: DebugName,
}

impl RawSuffix {
    pub fn from(alg: &str) -> Self {
        let alg_parts: Vec<&str> = alg.split(super::HASHER_NAME_DELIMITER).collect();
        debug_assert!(alg_parts.len() == 2);
        debug_assert_eq!(alg_parts[0], super::NAME_RAWSUFFIX);

        let delimiter = super::key_delimiter_name_2u8(alg, alg_parts[1]);
        Self {
            delimiter,
            name: alg.into(),
        }
    }
}

impl super::Hash for RawSuffix {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        // 按业务要求，如果没有分隔符，或者后缀有非数字，统统按照0处理
        let mut hash = 0i64;
        let mut found_delimiter = false;

        for i in 0..key.len() {
            let b = key.at(i);
            if !found_delimiter {
                if b == self.delimiter {
                    found_delimiter = true;
                }
                continue;
            }
            if !b.is_ascii_digit() {
                hash = 0;
                break;
            }
            hash = hash.wrapping_mul(10) + (b - '0' as u8) as i64;
        }

        hash
    }
}

impl Display for RawSuffix {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.name)
    }
}
