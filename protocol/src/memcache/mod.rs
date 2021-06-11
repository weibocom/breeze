mod binary;
pub use binary::{
    MemcacheBinary as Memcache, MemcacheBinaryKeyRoute as MemcacheRoute,
    MemcacheBinaryMetaStream as MemcacheMetaStream, MemcacheBinaryOpRoute as MemcacheOpRoute,
    MemcacheBinaryResponseParser as MemcacheResponseParser,
};
