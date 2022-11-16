use enum_dispatch::enum_dispatch;

use sharding::hash::Hash;

use crate::memcache::MemcacheBinary;
use crate::msgque::MsgQue;
use crate::phantom::Phantom;
use crate::redis::Redis;
use crate::{CbMetrics, Error, Flag, Result};

#[enum_dispatch(Proto)]
#[derive(Clone)]
pub enum Parser {
    McBin(MemcacheBinary),
    Redis(Redis),
    Phantom(Phantom),
    MsgQue(MsgQue),
}
impl Parser {
    pub fn try_from(name: &str) -> Result<Self> {
        match name {
            "mc" => Ok(Self::McBin(Default::default())),
            "redis" => Ok(Self::Redis(Default::default())),
            "phantom" => Ok(Self::Phantom(Default::default())),
            "msgque" => Ok(Self::MsgQue(Default::default())),
            _ => Err(Error::ProtocolNotSupported),
        }
    }
}
#[enum_dispatch]
pub trait Proto: Unpin + Clone + Send + Sync + 'static {
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()>;
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>>;
    // 返回nil convert的数量
    fn write_response<C: Commander, W: crate::Writer>(
        &self,
        ctx: &mut C,
        w: &mut W,
        metrics: &mut CbMetrics,
    ) -> Result<usize>;
    // 返回nil convert的数量
    #[inline]
    fn write_no_response<W: crate::Writer, F: Fn(i64) -> usize>(
        &self,
        _req: &HashedCommand,
        _w: &mut W,
        _dist_fn: F,
    ) -> Result<usize> {
        Err(Error::NoResponseFound)
    }
    #[inline]
    fn check(&self, _req: &HashedCommand, _resp: &Command) -> bool {
        true
    }
    // 构建回写请求。
    // 返回None: 说明req复用，build in place
    // 返回新的request
    fn build_writeback_request<C: Commander>(&self, _ctx: &mut C, _: u32) -> Option<HashedCommand> {
        todo!("not implement");
    }
    // 当前资源是否为cache。用来统计命中率
    #[inline]
    fn cache(&self) -> bool {
        false
    }
}

pub trait RequestProcessor {
    // last: 如果非multi-key cmd，则直接为true；
    // 如果是multi-key cmd，则满足以下所有条件时为true:
    // 1. 当前请求是multiget请求；
    // 2. 请求被拆分成了多个子请求；
    // 3. 当前子请求为最后一个；
    fn process(&mut self, req: HashedCommand, last: bool);
}

pub trait Stream {
    fn len(&self) -> usize;
    //fn at(&self, idx: usize) -> u8;
    fn slice(&self) -> ds::RingSlice;
    //fn update(&mut self, idx: usize, val: u8);
    fn take(&mut self, n: usize) -> ds::MemGuard;
    #[inline]
    fn ignore(&mut self, n: usize) {
        let _ = self.take(n);
    }
    // 在解析一个流的不同的req/response时，有时候需要共享数据。
    fn context(&mut self) -> &mut u64;
    // 用于保存下一个cmd需要使用的hash
    fn reserved_hash(&mut self) -> &mut i64;
    fn reserve(&mut self, r: usize);
}

pub trait Builder {
    type Endpoint: crate::Endpoint;
    fn build(&self) -> Self::Endpoint;
}

pub struct Command {
    flag: Flag,
    cmd: ds::MemGuard,
}

pub const MAX_DIRECT_HASH: i64 = i64::MAX;

pub struct HashedCommand {
    hash: i64,
    cmd: Command,
}

impl Command {
    #[inline]
    pub fn new(flag: Flag, cmd: ds::MemGuard) -> Self {
        Self { flag, cmd }
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.cmd.len()
    }
    #[inline]
    pub fn read(&self, oft: usize) -> &[u8] {
        self.cmd.read(oft)
    }
    #[inline]
    pub fn data(&self) -> &ds::RingSlice {
        self.cmd.data()
    }
    #[inline]
    pub fn data_mut(&mut self) -> &mut ds::RingSlice {
        self.cmd.data_mut()
    }
}
impl std::ops::Deref for Command {
    type Target = Flag;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.flag
    }
}
impl std::ops::DerefMut for Command {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.flag
    }
}

impl std::ops::Deref for HashedCommand {
    type Target = Command;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cmd
    }
}
impl std::ops::DerefMut for HashedCommand {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cmd
    }
}
use ds::MemGuard;
impl HashedCommand {
    #[inline]
    pub fn new(cmd: MemGuard, hash: i64, flag: Flag) -> Self {
        Self {
            hash,
            cmd: Command { flag, cmd },
        }
    }
    #[inline]
    pub fn hash(&self) -> i64 {
        self.hash
    }
    #[inline]
    pub fn update_hash(&mut self, idx_hash: i64) {
        if self.direct_hash() {
            self.hash = idx_hash;
        } else {
            log::warn!("should not update hash for non direct_hash!");
        }
    }
    #[inline]
    pub fn set_ignore_rsp(&mut self, ignore_rsp: bool) {
        self.cmd.set_ignore_rsp(ignore_rsp)
    }
    #[inline]
    pub fn master_only(&self) -> bool {
        self.cmd.master_only()
    }
}
impl AsRef<Command> for HashedCommand {
    #[inline]
    fn as_ref(&self) -> &Command {
        &self.cmd
    }
}

use std::fmt::{self, Debug, Display, Formatter};
use std::sync::Arc;
impl Display for HashedCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "hash:{} {}", self.hash, self.cmd)
    }
}
impl Debug for HashedCommand {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "hash:{} data:{:?}", self.hash, self.cmd)
    }
}
impl Display for Command {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "flag:{:?} len:{} sentonly:{} {} ok:{} op:{}/{:?}",
            self.flag,
            self.len(),
            self.sentonly(),
            self.cmd,
            self.ok(),
            self.op_code(),
            self.operation()
        )
    }
}
impl Debug for Command {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "flag:{:?} len:{} sentonly:{} {} ok:{} op:{}/{:?} data:{:?}",
            self.flag,
            self.len(),
            self.sentonly(),
            self.cmd,
            self.ok(),
            self.op_code(),
            self.operation(),
            self.data()
        )
    }
}

pub trait Commander {
    fn request_mut(&mut self) -> &mut HashedCommand;
    fn request(&self) -> &HashedCommand;
    fn response(&self) -> &Command;
    fn response_mut(&mut self) -> &mut Command;
}
