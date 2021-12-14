use crate::Protocol;

#[derive(Clone)]
pub struct RedisText;
//impl Protocol for RedisText {}
impl RedisText {
    pub fn new() -> Self {
        RedisText
    }
}
