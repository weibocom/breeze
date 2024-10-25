/// <pre>用于支持key中部分字符串做hashkey，且hash算法类似bkdr的hash算法；
/// hashkey是‘#’之后、$delimiter之前的内容；
/// 例如key：abc#123_456
///   如果$delimiter是'_'，则hashkey是123；
///   如果$delimiter是'^'，则hashkey是123_456；
/// 格式注意：'#'需要存在，否则hashkey为空；$delimiter可能不存在，如果$delimiter不存在，则'#'之后的全部是hashkey</pre>
#[derive(Clone, Default, Debug)]
pub struct BkdrsubDelimiter {
    delimiter: u8,
}
impl BkdrsubDelimiter {
    pub fn from(delimiter: u8) -> Self {
        Self { delimiter }
    }
}

impl super::Hash for BkdrsubDelimiter {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        const SEED: i32 = 131; // 31 131 1313 13131 131313 etc..
        const START_CHAR_VAL: u8 = '#' as u8;
        let end_char_val: u8 = self.delimiter;

        let mut hash = 0_i32;
        let mut found_start_char = false;
        // 轮询key中‘#’之后、‘_’之前的部分hashkey，如果没有'_'则一直计算到最后
        for i in 0..key.len() {
            let c = key.at(i);
            if found_start_char {
                // hashkey 计算
                if c != end_char_val {
                    hash = hash.wrapping_mul(SEED).wrapping_add(c as i32);
                    continue;
                }
                // 找到'_'，hashkey计算完毕
                break;
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
