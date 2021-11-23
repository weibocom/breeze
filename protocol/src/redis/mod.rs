mod binary;
mod text;

pub use binary::RedisBinary as RedisBin;
pub use text::RedisText;
#[derive(Debug, PartialEq)]
pub enum Command {}
