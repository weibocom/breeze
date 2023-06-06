use ds::RingSlice;

pub(super) use crate::memcache::packet::*;

pub use crate::memcache::packet::{Binary, OP_ADD};

use super::common::constants::Command;

pub(super) trait MysqlBinary {
    fn mysql_cmd(&self) -> Command;
}

impl MysqlBinary for RingSlice {
    #[inline(always)]
    fn mysql_cmd(&self) -> Command {
        Command::COM_QUERY
    }
}
