use crate::Protocol;

#[derive(Clone)]
pub struct RedisBinary;
//impl Protocol for RedisBinary {}
impl RedisBinary {
    pub fn new() -> Self {
        RedisBinary
    }
}
