mod binary;
mod text;

pub use binary::MemcacheBinary as MemcacheBin;
pub use text::MemcacheText;

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
