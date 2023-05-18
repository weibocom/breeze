// 基于rust_mysql_common调整而来，将基于connection的parse、build，改为基于vec<u8> 的方式打通；
// TODO 注意：目前存在多一次复制，需要进一步改为基于RingSlice进行解析的方式。

#[macro_use]
pub(crate) mod bitflags_ext;
pub(crate) mod value;

pub(crate) mod buffer_pool;
pub(crate) mod constants;
pub(crate) mod error;
pub(crate) mod io;
pub(crate) mod misc;
pub(crate) mod named_params;
pub(crate) mod opts;
pub(crate) mod packets;
pub(crate) mod params;
pub(crate) mod proto;
pub(crate) mod query_result;
pub(crate) mod row;
pub(crate) mod scramble;
