use enum_dispatch::enum_dispatch;

use sharding::hash::Hash;

use crate::kv::Kv;
use crate::memcache::MemcacheBinary;
use crate::msgque::MsgQue;
use crate::redis::Redis;
use crate::{Error, Flag, OpCode, Operation, Result, Stream, Writer};

#[derive(Clone)]
#[enum_dispatch(Proto)]
pub enum Parser {
    McBin(MemcacheBinary),
    Redis(Redis),
    MsgQue(MsgQue),
    // TODO 暂时保留，待client修改上线完毕后，清理
    // Mysql(Kv),
    Kv(Kv),
}
impl Parser {
    pub fn try_from(name: &str) -> Result<Self> {
        match name {
            "mc" => Ok(Self::McBin(Default::default())),
            "redis" | "phantom" => Ok(Self::Redis(Default::default())),
            "msgque" => Ok(Self::MsgQue(Default::default())),
            // "mysql" => Ok(Self::Mysql(Default::default())),
            "kv" => Ok(Self::Kv(Default::default())),
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
            // Self::Mysql(_) => false,
            Self::Kv(_) => false,
        }
    }
}

// #[derive(Default)]
// pub enum AuthMethod {}

#[derive(Default, Clone)]
pub struct ResOption {
    // pub method: AuthMethod,
    pub token: String,
    pub username: String,
}

pub enum HandShake {
    Success,
    Failed,
    Continue,
}

#[enum_dispatch]
pub trait Proto: Unpin + Clone + Send + Sync + 'static {
    //todo, Stream和Writer合并，ResOption用一个字段？
    fn handshake(&self, _stream: &mut impl Stream, _option: &mut ResOption) -> Result<HandShake> {
        Ok(HandShake::Success)
    }
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()>;
    fn build_request(&self, _req: &mut HashedCommand, _new_req: String) {}

    // fn before_send<S: Stream, Req: Request>(&self, _stream: &mut S, _req: &mut Req) {}

    // TODO: mysql debug专用，2023.7后可以清理 fishermen
    // fn parse_response_debug<S: Stream>(
    //     &self,
    //     _req: &HashedCommand,
    //     _data: &mut S,
    // ) -> Result<Option<Command>> {
    //     // TODO: just for debug
    //     Err(Error::NotInit)
    // }

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
    fn need_auth(&self) -> bool {
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

pub struct Command {
    ok: bool,
    cmd: MemGuard,
}

pub const MAX_DIRECT_HASH: i64 = i64::MAX;

pub struct HashedCommand {
    hash: i64,
    flag: Flag,
    cmd: ds::MemGuard,
    origin_cmd: Option<MemGuard>,
}

impl Command {
    //#[inline]
    //pub fn new(flag: Flag, cmd: ds::MemGuard) -> Self {
    //    Self { ok: flag.ok(), cmd }
    //}
    #[inline]
    pub fn from(ok: bool, cmd: ds::MemGuard) -> Self {
        Self { ok, cmd }
    }
    // #[inline]
    // pub fn new(flag: Flag, cmd: ds::MemGuard) -> Self {
    //     Self {
    //         ok: Ok(flag),
    //         cmd,
    //         origin_cmd: None,
    //     }
    // }
    pub fn from_ok(cmd: ds::MemGuard) -> Self {
        Self::from(true, cmd)
    }

    #[inline]
    pub fn ok(&self) -> bool {
        self.ok
    }

    //#[inline]
    //pub fn len(&self) -> usize {
    //    self.cmd.len()
    //}
    //#[inline]
    //pub fn read(&self, oft: usize) -> &[u8] {
    //    self.cmd.read(oft)
    //}

    // 不再暴露cmd，需要保存原有的cmd
    // #[inline]
    // pub fn cmd(&mut self) -> &mut MemGuard {
    //     &mut self.cmd
    // }
    // 获取origin data，调用方必须确保其存在

    //#[inline]
    //pub fn data(&self) -> &ds::RingSlice {
    //    self.cmd.data()
    //}
    //#[inline]
    //pub fn data_mut(&mut self) -> &mut ds::RingSlice {
    //    self.cmd.data_mut()
    //}
}
impl std::ops::Deref for Command {
    type Target = MemGuard;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cmd
    }
}
impl std::ops::DerefMut for Command {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cmd
    }
}

impl std::ops::Deref for HashedCommand {
    type Target = MemGuard;
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
            flag,
            cmd,
            origin_cmd: None,
        }
    }
    #[inline]
    pub fn reset_flag(&mut self, op_code: u16, op: Operation) {
        self.flag.reset_flag(op_code, op);
    }
    #[inline]
    pub fn hash(&self) -> i64 {
        self.hash
    }
    #[inline]
    pub fn update_hash(&mut self, idx_hash: i64) {
        self.hash = idx_hash;
    }
    //#[inline]
    //pub fn data(&self) -> &ds::RingSlice {
    //    self.cmd.data()
    //}
    //#[inline]
    //pub fn data_mut(&mut self) -> &mut ds::RingSlice {
    //    self.cmd.data_mut()
    //}
    //#[inline]
    //pub fn len(&self) -> usize {
    //    self.cmd.len()
    //}
    #[inline]
    pub fn sentonly(&self) -> bool {
        self.flag.sentonly()
    }
    #[inline]
    pub fn set_sentonly(&mut self, v: bool) {
        self.flag.set_sentonly(v);
    }
    #[inline]
    pub fn operation(&self) -> Operation {
        self.flag.operation()
    }
    #[inline]
    pub fn op_code(&self) -> OpCode {
        self.flag.op_code()
    }
    #[inline]
    pub fn noforward(&self) -> bool {
        self.flag.noforward()
    }
    #[inline]
    pub fn flag(&self) -> &Flag {
        &self.flag
    }
    #[inline]
    pub fn flag_mut(&mut self) -> &mut Flag {
        &mut self.flag
    }
    #[inline]
    pub(crate) fn origin_data(&self) -> &MemGuard {
        if let Some(origin) = &self.origin_cmd {
            origin
        } else {
            panic!("origin is null, req:{:?}", self.cmd.data())
        }
    }
    #[inline]
    pub fn reshape(&mut self, mut dest_cmd: MemGuard) {
        assert!(
            self.origin_cmd.is_none(),
            "origin cmd should be none: {:?}",
            self.origin_cmd
        );
        // 将dest cmd设给cmd，并将换出的cmd保留在origin_cmd中
        mem::swap(&mut self.cmd, &mut dest_cmd);
        self.origin_cmd = Some(dest_cmd);
    }
    //#[inline]
    //pub fn try_next_type(&self) -> TryNextType {
    //    self.flag.try_next_type()
    //}
}
//impl AsRef<Command> for HashedCommand {
//    #[inline]
//    fn as_ref(&self) -> &Command {
//        &self.cmd
//    }
//}

use std::fmt::{self, Debug, Display, Formatter};
use std::mem;
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
        write!(f, "ok:{} {}", self.ok, self.cmd)
    }
}
impl Debug for Command {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ok:{} {:?}", self.ok, self.cmd)
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
