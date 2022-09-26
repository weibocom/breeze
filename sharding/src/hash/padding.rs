use super::Hash;

#[derive(Clone, Default, Debug)]
pub struct Padding;

impl Hash for Padding {
    fn hash<S: super::HashKey>(&self, _key: &S) -> i64 {
        log::warn!(
            "+++ careful - may not call the padding hash with key: {:?}",
            _key
        );
        0
    }
}
