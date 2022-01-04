use enum_dispatch::enum_dispatch;

use sharding::hash::Hash;

use crate::memcache::MemcacheBinary;
use crate::redis::Redis;
use crate::{Error, Flag, Result};
#[enum_dispatch(Proto)]
#[derive(Clone)]
pub enum Parser {
    McBin(MemcacheBinary),
    Redis(Redis),
}
impl Parser {
    pub fn try_from(name: &str) -> Result<Self> {
        match name {
            "mc" => Ok(Self::McBin(Default::default())),
            "redis" => Ok(Self::Redis(Default::default())),
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
    fn write_response<C: Commander, W: crate::ResponseWriter>(
        &self,
        ctx: &mut C,
        w: &mut W,
    ) -> Result<()>;
    fn write_no_response<W: crate::ResponseWriter>(
        &self,
        _req: &HashedCommand,
        _w: &mut W,
    ) -> Result<()> {
        Err(Error::NoResponseFound)
    }
    // 构建回写请求。
    // 返回None: 说明req复用，build in place
    // 返回新的request
    fn build_writeback_request<C: Commander>(&self, _ctx: &mut C, _: u32) -> Option<HashedCommand> {
        todo!("not implement");
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
    fn at(&self, idx: usize) -> u8;
    fn slice(&self) -> ds::RingSlice;
    fn update(&mut self, idx: usize, val: u8);
    fn take(&mut self, n: usize) -> ds::MemGuard;
    #[inline(always)]
    fn ignore(&mut self, n: usize) {
        let _ = self.take(n);
    }
    // 在解析一个流的不同的req/response时，有时候需要共享数据。
    fn context(&mut self) -> &mut u64;
}

pub trait Builder {
    type Endpoint: crate::Endpoint;
    fn build(&self) -> Self::Endpoint;
}

pub struct Command {
    flag: Flag,
    cmd: ds::MemGuard,
}

pub struct HashedCommand {
    hash: i64,
    cmd: Command,
}

impl Command {
    #[inline(always)]
    pub fn new(flag: Flag, key_count: u16, cmd: ds::MemGuard) -> Self {
        Self { flag, cmd }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.cmd.len()
    }
    #[inline(always)]
    pub fn read(&self, oft: usize) -> &[u8] {
        self.cmd.read(oft)
    }
    #[inline(always)]
    pub fn data(&self) -> &ds::RingSlice {
        self.cmd.data()
    }
    #[inline(always)]
    pub fn data_mut(&mut self) -> &mut ds::RingSlice {
        self.cmd.data_mut()
    }
}
impl std::ops::Deref for Command {
    type Target = Flag;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.flag
    }
}
impl std::ops::DerefMut for Command {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.flag
    }
}

impl std::ops::Deref for HashedCommand {
    type Target = Command;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.cmd
    }
}
impl std::ops::DerefMut for HashedCommand {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cmd
    }
}
use ds::MemGuard;
impl HashedCommand {
    #[inline(always)]
    pub fn new(cmd: MemGuard, hash: i64, flag: Flag) -> Self {
        Self {
            hash,
            cmd: Command { flag, cmd },
        }
    }
    #[inline(always)]
    pub fn hash(&self) -> i64 {
        self.hash
    }
}
impl AsRef<Command> for HashedCommand {
    #[inline(always)]
    fn as_ref(&self) -> &Command {
        &self.cmd
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for HashedCommand {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "hash:{} {}", self.hash, self.cmd)
    }
}
impl Display for Command {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "flag:{:?} len:{} sentonly:{} {} ok:{} op code:{} op:{:?}",
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
        Display::fmt(self, f)
    }
}

pub trait Commander {
    fn request_mut(&mut self) -> &mut HashedCommand;
    fn request(&self) -> &HashedCommand;
    fn response(&self) -> &Command;
    fn response_mut(&mut self) -> &mut Command;
}
