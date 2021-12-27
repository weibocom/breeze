use enum_dispatch::enum_dispatch;

use sharding::hash::Hash;

use crate::memcache::MemcacheBinary;
use crate::redis::Redis;
use crate::{Error, Operation, Result};
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
    // last: 满足以下所有条件时为true:
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
}

pub trait Builder {
    type Endpoint: crate::Endpoint;
    fn build(&self) -> Self::Endpoint;
}

pub struct Flag {
    v: u64,
}

// 高5字节，归各种协议私用，低3个字节，共享
const RSP_BIT_META_LEN: u8 = 40;
const RSP_BIT_TOKEN_LEN: u8 = 48;
const REQ_BIT_PADDING_RSP: u8 = 32;
const REQ_BIT_MKEY_FIRST: u8 = 24;
// const REQ_BIT_VAL_MKEY_FIRST: u64 = 1 << 24;

impl Flag {
    // first = true 满足所有条件1. 当前请求是multiget；2. 拆分了多个子请求；3. 是`第一`个子请求；
    // last  = true 满足所有条件1. 当前请求是multiget；2. 拆分了多个子请求；3. 是`最后`一个子请求；
    #[inline(always)]
    pub fn from_op(op_code: u8, op: Operation) -> Self {
        let v = ((op_code as u64) << 8) | (op as u64);
        Self { v }
    }

    // **********************  TODO: redis相关，需要按协议分拆 start **********************
    // TODO: 后续flag需要根据协议进行分别构建 speedup fishermen
    #[inline(always)]
    pub fn from_mkey_op(mkey_first: bool, padding_rsp: u8, op: Operation) -> Self {
        let mut v = 0u64;
        // 只有多个key时，first key才有意义
        if mkey_first {
            const MKEY_FIRST_VAL: u64 = 1 << REQ_BIT_MKEY_FIRST;
            v |= MKEY_FIRST_VAL;
        }

        if padding_rsp > 0 {
            v |= (padding_rsp as u64) << REQ_BIT_PADDING_RSP;
        }

        // operation 需要小雨
        v |= op as u64;

        Self { v }
    }

    // meta len 正常不会超过20，256绰绰有余
    pub fn from_metalen_tokencount(metalen: u8, tokencount: u8) -> Self {
        let mut v = 0u64;
        if metalen > 0 {
            v |= (metalen as u64) >> RSP_BIT_META_LEN;
        }
        if tokencount > 0 {
            v |= (tokencount as u64) >> RSP_BIT_TOKEN_LEN;
        }
        Self { v }
    }

    pub fn padding_rsp(&self) -> u8 {
        const MASK: u64 = 1 << REQ_BIT_PADDING_RSP - 1;
        let p = (self.v >> REQ_BIT_PADDING_RSP) & MASK;
        p as u8
    }

    pub fn is_mkey_first(&self) -> bool {
        const MASK_MKF: u64 = 1 << REQ_BIT_MKEY_FIRST;
        (self.v & MASK_MKF) == MASK_MKF
    }

    pub fn meta_len(&self) -> u8 {
        const MASK: u64 = 1 << RSP_BIT_META_LEN - 1;
        let len = self.v >> RSP_BIT_META_LEN & MASK;
        len as u8
    }

    pub fn token_len(&self) -> u8 {
        const MASK: u64 = 1u64 << RSP_BIT_TOKEN_LEN - 1;
        let len = self.v >> RSP_BIT_TOKEN_LEN & MASK;
        len as u8
    }

    // **********************  TODO: redis相关，需要按协议分拆 end！speedup **********************

    #[inline(always)]
    pub fn new() -> Self {
        Self { v: 0 }
    }
    // 低位第一个字节是operation位
    // 第二个字节是op_code
    const STATUS_OK: u8 = 16;
    const SEND_ONLY: u8 = 17;
    const NO_FORWARD: u8 = 18;

    #[inline(always)]
    pub fn set_status_ok(&mut self) -> &mut Self {
        self.mark(Self::STATUS_OK);
        self
    }
    #[inline(always)]
    pub fn ok(&self) -> bool {
        self.marked(Self::STATUS_OK)
    }
    #[inline(always)]
    pub fn set_sentonly(&mut self) -> &mut Self {
        self.mark(Self::SEND_ONLY);
        self
    }
    #[inline(always)]
    pub fn sentonly(&self) -> bool {
        self.marked(Self::SEND_ONLY)
    }
    #[inline(always)]
    pub fn operation(&self) -> Operation {
        (self.v as u8).into()
    }
    #[inline(always)]
    pub fn op_code(&self) -> u8 {
        // 第二个字节是op_code
        (self.v >> 8) as u8
    }
    #[inline(always)]
    pub fn set_noforward(&mut self) -> &mut Self {
        self.mark(Self::NO_FORWARD);
        self
    }
    #[inline(always)]
    pub fn noforward(&self) -> bool {
        self.marked(Self::NO_FORWARD)
    }

    #[inline(always)]
    pub fn mark(&mut self, bit: u8) {
        self.v |= 1 << bit;
    }
    #[inline(always)]
    pub fn marked(&self, bit: u8) -> bool {
        let m = 1 << bit;
        self.v & m == m
    }
    #[inline(always)]
    pub fn reset_flag(&mut self, op_code: u8, op: Operation) {
        *self = Self::from_op(op_code, op);
    }
}

pub struct Command {
    flag: Flag,
    key_count: u16,
    cmd: ds::MemGuard,
}

pub struct HashedCommand {
    hash: u64,
    cmd: Command,
}

impl Command {
    #[inline(always)]
    pub fn new(flag: Flag, key_count: u16, cmd: ds::MemGuard) -> Self {
        Self {
            flag,
            key_count,
            cmd,
        }
    }
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.cmd.len()
    }
    #[inline(always)]
    pub fn key_count(&self) -> u16 {
        self.key_count
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
    pub fn new(cmd: MemGuard, hash: u64, flag: Flag, key_count: u16) -> Self {
        Self {
            hash,
            cmd: Command {
                flag,
                key_count,
                cmd,
            },
        }
    }
    #[inline(always)]
    pub fn hash(&self) -> u64 {
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
            "flag:{} len:{} sentonly:{} {} ok:{} op code:{} op:{:?}",
            self.flag.v,
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
