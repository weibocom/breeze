#[derive(Clone, Default, Debug)]
pub struct Bkdr;

//TODO 参考java版本调整，手动测试各种长度key，hash一致，需要线上继续验证 fishermen
impl super::Hash for Bkdr {
    fn hash<K: super::HashKey>(&self, b: &K) -> i64 {
        let mut h = 0i32;
        let seed = 31i32;
        for i in 0..b.len() {
            h = h.wrapping_mul(seed).wrapping_add(b.at(i) as i32);
        }
        if h < 0 {
            h = h.wrapping_mul(-1);
        }

        // 由于计算过程有溢出，部分key最终的hash可以为0，eg："2993365881.sup"
        // if h == 0 {
        //     log::error!("bkdr - found zero hash for key: {:?}", b);
        // }

        h as i64
    }
}
