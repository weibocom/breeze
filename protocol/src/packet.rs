use crate::{Flag, OpCode, Operation};
use ds::MemGuard;
use std::ops::{Deref, DerefMut};

pub type Command = ResponsePacket;
pub type HashedCommand = RequestPacket;

pub struct ResponsePacket {
    ok: bool,
    cmd: MemGuard,
}

pub struct RequestPacket {
    hash: i64,
    flag: Flag,
    cmd: MemGuard,
    origin_cmd: Option<MemGuard>,
}

impl ResponsePacket {
    #[inline]
    pub fn from(ok: bool, cmd: MemGuard) -> Self {
        Self { ok, cmd }
    }
    pub fn from_ok(cmd: MemGuard) -> Self {
        Self::from(true, cmd)
    }
    #[inline]
    pub fn ok(&self) -> bool {
        self.ok
    }
}
impl Deref for ResponsePacket {
    type Target = MemGuard;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cmd
    }
}
impl DerefMut for ResponsePacket {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cmd
    }
}

impl Deref for RequestPacket {
    type Target = MemGuard;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.cmd
    }
}
impl DerefMut for RequestPacket {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cmd
    }
}
impl RequestPacket {
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
    pub fn origin_data(&self) -> &MemGuard {
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
}

use std::fmt::{self, Debug, Display, Formatter};
use std::mem;
impl Display for RequestPacket {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "hash:{} {}", self.hash, self.cmd)
    }
}
impl Debug for RequestPacket {
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
