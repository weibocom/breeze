/// <pre>用于支持key中部分字符串做hashkey，且hash算法类似bkdr的hash算法；
/// key格式：abc#123_456，hashkey则是‘#’之后、‘_’之前的内容；
/// 格式注意：'#'需要存在，否则hashkey为空；'_'可能不存在，如果'_'不存在，则'#'之后的全部是hashkey</pre>
#[derive(Clone, Default, Debug)]
pub struct Bkdrsub;

impl super::Hash for Bkdrsub {
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        const SEED: i32 = 131; // 31 131 1313 13131 131313 etc..
        const START_CHAR_VAL: u8 = '#' as u8;
        const END_CHAR_VAL: u8 = '_' as u8;

        let mut hash = 0_i32;
        let mut found_start_char = false;
        // 轮询key中‘#’之后、‘_’之前的部分hashkey，如果没有'_'则一直计算到最后
        for i in 0..key.len() {
            let c = key.at(i);
            if found_start_char {
                // hashkey 计算
                if c != END_CHAR_VAL {
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
