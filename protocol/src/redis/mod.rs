mod resp2;

pub use resp2::RedisResp2;
#[derive(Debug, PartialEq)]
pub enum Command {
    Get,
    Mget,
    Gets,
    Set,
    Cas,
    Add,
    Version,
    Unknown,
}
