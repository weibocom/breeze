use super::Hash;

#[derive(Clone, Default, Debug)]
pub struct Padding;

impl Hash for Padding {
    #[inline(always)]
    fn hash<S: super::HashKey>(&self, key: &S) -> i64 {
        log::warn!("+++ padding hash with key: {:?}", key);
        0
    }
}
