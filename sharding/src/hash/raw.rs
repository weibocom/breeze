#[derive(Clone, Default, Debug)]
pub struct Raw;

impl super::Hash for Raw {
    // key需要是数字或者数字前缀，直接返回key代表的数字
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        let mut hash = 0;
        for i in 0..key.len() {
            if !key.at(i).is_ascii_digit() {
                return hash;
            }
            hash = hash * 10 + (key.at(i) - '0' as u8) as i64;
        }

        if hash <= 0 {
            log::error!("found malform hash/{} for key: {:?}", hash, key);
        }

        hash
    }
}
