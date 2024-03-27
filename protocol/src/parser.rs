use enum_dispatch::enum_dispatch;

use sharding::hash::Hash;

use crate::kv::Kv;
use crate::memcache::MemcacheBinary;
use crate::msgque::MsgQue;
use crate::redis::Redis;
use crate::uuid::Uuid;
use crate::vector::Vector;
use crate::{Command, Error, HashedCommand, Result, Stream, Writer};

#[derive(Clone)]
#[enum_dispatch(Proto)]
pub enum Parser {
    McBin(MemcacheBinary),
    Redis(Redis),
    MsgQue(MsgQue),
    // TODO 暂时保留，待client修改上线完毕后，清理
    // Mysql(Kv),
    Kv(Kv),
    Uuid(Uuid),
    Vector(Vector),
}
impl Parser {
    pub fn try_from(name: &str) -> Result<Self> {
        match name {
            "mc" => Ok(Self::McBin(Default::default())),
            "redis" | "phantom" => Ok(Self::Redis(Default::default())),
            "msgque" => Ok(Self::MsgQue(Default::default())),
            "kv" => Ok(Self::Kv(Default::default())),
            "uuid" => Ok(Self::Uuid(Default::default())),
            "vector" => Ok(Self::Vector(Default::default())),
            _ => Err(Error::ProtocolNotSupported),
        }
    }
}

#[derive(Default, Clone)]
pub struct ResOption {
    pub token: String,
    pub username: String,
}

#[derive(Default, Clone)]
pub struct Config {
    pub need_auth: bool,
    pub pipeline: bool,
    //pub retry_on_rsp_notok: bool,
}

pub enum HandShake {
    Success,
    Failed,
    Continue,
}

#[enum_dispatch]
pub trait Proto: Unpin + Clone + Send + Sync + 'static {
    #[allow(unused_variables)]
    fn handshake(&self, stream: &mut impl Stream, option: &mut ResOption) -> Result<HandShake> {
        Ok(HandShake::Success)
    }
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()>;
    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>>;
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

    // TODO check逻辑，当前分为assert、默认不处理两种方式，协议还需要额外的validate来进行校验；
    // 更佳的方式是返回Error，通过Error框架，来统一处理异常？从而整合掉check和validate fishermen
    #[inline]
    fn check(&self, _req: &HashedCommand, _resp: &Command) {}
    // 构建回写请求。
    // 返回None: 说明req复用，build in place
    // 返回新的request
    #[inline(always)]
    fn build_writeback_request<C, M, I>(
        &self,
        _ctx: &mut C,
        _response: &Command,
    ) -> Option<HashedCommand>
    where
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        None
    }
    fn config(&self) -> Config {
        Config::default()
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

pub const MAX_DIRECT_HASH: i64 = i64::MAX;

pub trait Commander<M: Metric<I>, I> {
    fn request_mut(&mut self) -> &mut HashedCommand;
    fn request(&self) -> &HashedCommand;
    // 请求所在的分片位置
    fn request_shard(&self) -> usize;
    fn metric(&self) -> &M;
    fn ctx(&self) -> u64;
}

pub struct MetricName;

pub trait Metric<I> {
    fn cache(&self, hit: bool);
    fn inconsist(&self);
}
pub trait MetricItem: std::ops::AddAssign<i64> + std::ops::AddAssign<bool> {}
impl<T: std::ops::AddAssign<i64> + std::ops::AddAssign<bool>> MetricItem for T {}
