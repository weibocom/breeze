use super::DebugName;
use std::fmt::Display;

/// <pre>用于支持key中部分字符串做hashkey，且hash算法类似bkdrsub的hash算法；
/// bkdrsubstr-$markstr
/// hashkey的计算方式：hashkey则是‘#’之后、$markstr之前的内容；
/// 以bkdrsubstr-(((((为例
/// key格式：abc#123_456(((((789，hashkey是123_456
/// 格式注意：'#'需要存在，否则hashkey为空；'$markstr'可能不存在，如果'$markstr'不存在，则'#'之后的全部是hashkey</pre>
#[derive(Clone, Default, Debug)]
pub struct Bkdrsubstr {
    len: u8,
    markerstr: Vec<u8>,
    name: DebugName,
}

impl Bkdrsubstr {
    pub fn from(alg: &str) -> Self {
        let alg_parts: Vec<&str> = alg.split(super::HASHER_NAME_DELIMITER).collect();
        assert!(alg_parts.len() == 2);
        assert_eq!(alg_parts[0], "bkdrsubstr");
        let markerstr = alg_parts[1].as_bytes().to_vec();
        return Self {
            len: markerstr.len() as u8,
            markerstr,
            name: alg.into(),
        };
    }
}
impl super::Hash for Bkdrsubstr {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        const SEED: i32 = 131; // 31 131 1313 13131 131313 etc..
        const START_CHAR_VAL: u8 = '#' as u8;
        let mark_start_char: u8 = self.markerstr[0];

        let is_substr = |a: &S, b: &[u8], l, start| {
            for k in 1..l {
                if a.at(k + start) != b[k] {
                    return false;
                }
            }
            return true;
        };

        let mut hash = 0_i32;
        let mut found_start_char = false;
        // 轮询key中‘#’之后、‘_’之前的部分hashkey，如果没有'_'则一直计算到最后
        let key_len = key.len();
        for i in 0..key_len {
            let c = key.at(i);
            if found_start_char {
                // hashkey 计算
                if c == mark_start_char {
                    if (i + self.len as usize) < key_len {
                        if is_substr(key, &self.markerstr, self.len as usize, i) {
                            break;
                        }
                    }
                }
                hash = hash.wrapping_mul(SEED).wrapping_add(c as i32);
            } else if c == START_CHAR_VAL {
                found_start_char = true;
                continue;
            }
            // 没有找到#，持续轮询下一个字节
        }

        hash = hash & 0x7FFFFFFF;
        hash as i64
    }
}
impl Display for Bkdrsubstr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.name)
    }
}
