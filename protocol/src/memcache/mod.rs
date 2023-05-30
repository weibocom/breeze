mod binary;
pub(crate) use binary::packet;
//mod text;

pub use binary::Binary;
pub use binary::MemcacheBinary as MemcacheBin;
pub use binary::MemcacheBinary;
//pub use text::MemcacheText;

// #[derive(Debug, PartialEq)]
// pub enum Command {
//     Get,
//     Mget,
//     Gets,
//     Set,
//     Cas,
//     Add,
//     Version,
//     Unknown,
// }
