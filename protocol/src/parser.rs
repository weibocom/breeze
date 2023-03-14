use enum_dispatch::enum_dispatch;

use sharding::hash::Hash;

use crate::memcache::MemcacheBinary;
use crate::msgque::MsgQue;
use crate::redis::Redis;
use crate::{Error, Flag, Result, Stream, Writer};

#[enum_dispatch(Proto)]
#[derive(Clone)]
pub enum Parser {
    McBin(MemcacheBinary),
    Redis(Redis),
    MsgQue(MsgQue),
}
impl Parser {
    pub fn try_from(name: &str) -> Result<Self> {
        match name {
            "mc" => Ok(Self::McBin(Default::default())),
            "redis" | "phantom" => Ok(Self::Redis(Default::default())),
            "msgque" => Ok(Self::MsgQue(Default::default())),
            _ => Err(Error::ProtocolNotSupported),
        }
    }
    #[inline]
    pub fn pipeline(&self) -> bool {
        match self {
            Self::McBin(_) => false,
            Self::Redis(_) => true,
            // Self::Phantom(_) => true,
            Self::MsgQue(_) => false,
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

    // 根据req，构建本地response响应，全部无差别构建resp，具体quit或异常，在wirte response处处理
    // fn build_local_response<F: Fn(i64) -> usize>(&self, req: &HashedCommand, dist_fn: F)
    //     -> Command;

    fn write_response<C, W, M, I>(
        &self,
        ctx: &mut C,
        response: Option<&mut Command>,
        w: &mut W,
    ) -> Result<()>
    where
        W: Writer,
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem;

    #[inline]
    fn check(&self, _req: &HashedCommand, _resp: &Command) {}
    // 构建回写请求。
    // 返回None: 说明req复用，build in place
    // 返回新的request
    fn build_writeback_request<C, M, I>(
        &self,
        _ctx: &mut C,
        _response: &Command,
        _: u32,
    ) -> Option<HashedCommand>
    where
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        None
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

pub type ReservedHash = Option<i64>;

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
    //#[inline]
    //pub fn read(&self, oft: usize) -> &[u8] {
    //    self.cmd.read(oft)
    //}
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
        self.hash = idx_hash;
    }
}
impl AsRef<Command> for HashedCommand {
    #[inline]
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

pub trait Commander<M: Metric<I>, I: MetricItem> {
    fn request_mut(&mut self) -> &mut HashedCommand;
    fn request(&self) -> &HashedCommand;
    // response  单独拆除
    // fn response(&self) -> Option<&Command>;
    // fn response_mut(&mut self) -> Option<&mut Command>;
    // 请求所在的分片位置
    fn request_shard(&self) -> usize;
    fn metric(&self) -> &M;
}

pub enum MetricName {
    Read,
    Write,
    NilConvert,
    Cache, // cache(mc)命中率
    Inconsist,
}
pub trait Metric<Item: MetricItem> {
    fn get(&self, name: MetricName) -> &mut Item;
    #[inline]
    fn cache(&self, hit: bool) {
        *self.get(MetricName::Cache) += hit;
    }
    #[inline]
    fn inconsist(&self, c: i64) {
        *self.get(MetricName::Inconsist) += c;
    }
}
pub trait MetricItem: std::ops::AddAssign<i64> + std::ops::AddAssign<bool> {}
impl<T: std::ops::AddAssign<i64> + std::ops::AddAssign<bool>> MetricItem for T {}
