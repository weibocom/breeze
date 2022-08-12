// 随机值作为hash，使用场景：1）多写随机读； 2）随机写，轮询 or 随机读

#[derive(Clone, Default, Debug)]
pub struct RandomHash;

impl super::Hash for RandomHash {
    fn hash<S: super::HashKey>(&self, _key: &S) -> i64 {
        // hash 目前为u32，所以此处保持这个范围
        rand::random::<u32>() as i64
    }
}
