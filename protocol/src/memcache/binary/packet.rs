// // mc 二进制协议包，用于构建各种协议指令，所有mc协议构建均需放在这里 fishermen

// cmd的第一个字节，用于标示request or response
#[allow(dead_code)]
pub enum Magic {
    Request = 0x80,
    Response = 0x81,
}

pub const CAS_LEN: usize = 8;
use crate::{Error, Result, TryNextType};

// response status 共11种，协议中占2个字节，当前只有1字节，如果超范围需要在协议处理位置对应修改
#[allow(dead_code)]
pub enum RespStatus {
    NoError = 0x0000,
    NotFound = 0x0001,
    InvalidArg = 0x0004,
    NotStored = 0x0005,
    NonNumeric = 0x0006,
    // 扩展一个quit，用于支持关闭连接
    Quit = 0x0007,
    UnkownCmd = 0x0081,
    OutOfMemory = 0x0082,
}

use crate::Operation;
#[allow(dead_code)]
pub(crate) enum PacketPos {
    Magic = 0,
    Opcode = 1,
    Key = 2,
    ExtrasLength = 4,
    DataType = 5,
    Status = 6,
    TotalBodyLength = 8,
    Opaque = 12,
    Cas = 16,
}
pub(crate) const HEADER_LEN: usize = 24;

// 对应版本号为: 0.0.1
pub(crate) const VERSION_RESPONSE: [u8; 29] = [
    0x81, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x2e, 0x30, 0x2e, 0x31,
];
// EMPTY Response
pub(crate) const STAT_RESPONSE: [u8; 24] = [
    129, 0x10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

// https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped#command-opcodes
// MC包含Get, MGet, Gets, Store, Meta四类命令，索引分别是0-4
// 0x48 是Gets请求
pub(crate) const COMMAND_IDX: [u8; 128] = [
    0, 3, 3, 3, 3, 3, 3, 4, 4, 1, 1, 4, 0, 1, 0, 0, 4, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
// OP_CODE对应的noreply code。
// 注意：根据业务逻辑，add会转换成setq
// cas 变更为setq
pub(crate) const NOREPLY_MAPPING: [u8; 128] = [
    0x09, 0x11, 0x11, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x09, 0x00, 0x00, 0x0d, 0x0d, 0x19, 0x1a,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
    0x30, 0x32, 0x32, 0x34, 0x34, 0x36, 0x36, 0x38, 0x38, 0x3a, 0x3a, 0x3c, 0x3c, 0x3d, 0x3e, 0x3f,
    0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x49, 0x49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0,
];
// 哪些请求是不需要转发的. 比如noop请求，version, status, quit等请求。这些请求可以直接计算出response。
//pub(crate) const NO_FORWARD_OPS: [u8; 128] = [
//    0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
//    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//];

// 请求完毕后，不考虑layer及其他配置，如果cmd失败,是否继续try_next:
// (1) 0: not try next(对add/replace生效);  (2) 1: try next;  (3) 2:unkown (仅对set生效，注意提前考虑cas)
const TRY_NEXT_TABLE: [u8; 128] = [
    1, 2, 0, 0, 1, 1, 1, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 2, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 0,
    1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    0, 0, 1, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

// 总共有48个opcode，这里先只部分支持
#[allow(dead_code)]
#[repr(u8)]
pub enum Opcode {
    GET = OP_GET,
    NOOP = OP_NOOP,
    VERSION = OP_VERSION,
    STAT = OP_STAT,
    QUIT = OP_QUIT,
    QUITQ = OP_QUITQ,
    GETKQ = OP_GETKQ,
    GETQ = OP_GETQ,
    SET = OP_SET,
    DEL = OP_DEL,
    ADD = OP_ADD,
    GETK = OP_GETK,
    GETS = OP_GETS,
    GETSQ = OP_GETSQ,
    SETQ = OP_SETQ,
}

pub(crate) const REQUEST_MAGIC: u8 = 0x80;
pub(crate) const RESPONSE_MAGIC: u8 = 0x81;

pub(crate) const OP_GET: u8 = 0x00;
pub(crate) const OP_NOOP: u8 = 0x0a;
pub(crate) const OP_VERSION: u8 = 0x0b;
pub(crate) const OP_STAT: u8 = 0x10;
pub(crate) const OP_QUIT: u8 = 0x07;
pub(crate) const OP_QUITQ: u8 = 0x17;
pub(crate) const OP_GETKQ: u8 = 0x0d;
pub(crate) const OP_GETQ: u8 = 0x09;
pub(crate) const OP_SET: u8 = 0x01;
pub(crate) const OP_DEL: u8 = 0x04;
pub const OP_ADD: u8 = 0x02;
pub(crate) const OP_GETK: u8 = 0x0c;
pub(crate) const OP_SETQ: u8 = 0x11;
// 这个专门为gets扩展
pub const OP_GETS: u8 = 0x48;
// 这个没有业务使用，先注销掉
pub const OP_GETSQ: u8 = 0x49;

// 0x09: getq
// 0x0d: getkq
// 0x49: getsq
//pub(crate) const QUITE_GET_TABLE: [u8; 128] = [
//    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
//];
#[inline(always)]
pub(crate) fn is_quiet_get(op_code: u8) -> bool {
    match op_code {
        OP_GETQ | OP_GETKQ | OP_GETSQ => true,
        _ => false,
    }
    //QUITE_GET_TABLE[op_code as usize] == 1
}

// 在请求时，部分场景下把op_code进行一次映射。
// 1. quite get请求映射成 non-quite get请求。
//      getq(0x09) => get(0x00); getkq(0x0d) => getk(0x0c); 以实现multiget的pipeline
// 2. 把gets(0x48), getsq(0x49) => get(0x00)请求。 // 支持gets请求只发送给master
//pub(crate) const OPS_MAPPING_TABLE: [u8; 128] = [
//    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x00, 0x0a, 0x0b, 0x0c, 0x0c, 0x0e, 0x0f,
//    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
//    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
//    0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f,
//    0x40, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x00, 0x00, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f,
//    0x50, 0x51, 0x52, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x5b, 0x5c, 0x5d, 0x5e, 0x5f,
//    0x60, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f,
//    0x70, 0x71, 0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79, 0x7a, 0x7b, 0x7c, 0x7d, 0x7e, 0x7f,
//];

pub trait Binary {
    type Item;
    fn op(&self) -> u8;
    fn operation(&self) -> Operation;
    fn noop(&self) -> bool;
    fn extra_len(&self) -> u8;
    fn extra_or_flag(&self) -> Self::Item;
    fn total_body_len(&self) -> u32;
    fn opaque(&self) -> u32;
    fn cas(&self) -> u64;
    fn status_ok(&self) -> bool;
    fn key(&self) -> Self::Item;
    fn key_len(&self) -> u16;
    // 仅仅用于获取value长度，注意区分total body len
    fn value_len(&self) -> u32;
    fn value(&self) -> Self::Item;
    fn packet_len(&self) -> usize;
    // 是否为quite get请求。
    fn quiet_get(&self) -> bool;
    // 当前请求是否是quite请求
    fn is_quiet(&self) -> bool;
    fn clear_cas(&mut self);
    fn map_op(&mut self) -> u8;
    fn map_op_noreply(&mut self) -> u8;
    fn restore_op(&mut self, op: u8);
    fn hash<H: sharding::hash::Hash>(&self, alg: &H) -> i64;
    fn check_request(&self) -> Result<()>;
    fn check_response(&self) -> Result<()>;
    fn try_next_type(&self) -> TryNextType;
    fn sentonly(&self) -> bool;
    fn noforward(&self) -> bool;
}

pub trait Op {}

use ds::RingSlice;
impl Binary for RingSlice {
    type Item = Self;
    #[inline(always)]
    fn op(&self) -> u8 {
        debug_assert!(self.len() >= HEADER_LEN);
        self.at(PacketPos::Opcode as usize)
    }
    #[inline(always)]
    fn noop(&self) -> bool {
        self.op() == OP_NOOP
    }
    #[inline(always)]
    fn operation(&self) -> Operation {
        (COMMAND_IDX[self.op() as usize]).into()
    }
    #[inline(always)]
    fn extra_len(&self) -> u8 {
        debug_assert!(self.len() >= HEADER_LEN);
        self.at(PacketPos::ExtrasLength as usize)
    }
    #[inline(always)]
    fn extra_or_flag(&self) -> Self {
        // 读取flag时，需要有完整的packet
        debug_assert!(self.len() >= HEADER_LEN);
        let extra_len = self.extra_len() as usize;
        self.sub_slice(HEADER_LEN, extra_len)
    }
    #[inline(always)]
    fn total_body_len(&self) -> u32 {
        debug_assert!(self.len() >= HEADER_LEN);
        self.read_u32(PacketPos::TotalBodyLength as usize)
    }
    #[inline(always)]
    fn opaque(&self) -> u32 {
        debug_assert!(self.len() >= HEADER_LEN);
        self.read_u32(PacketPos::Opaque as usize)
    }
    #[inline(always)]
    fn cas(&self) -> u64 {
        debug_assert!(self.len() >= HEADER_LEN);
        self.read_u64(PacketPos::Cas as usize)
    }
    #[inline(always)]
    fn packet_len(&self) -> usize {
        debug_assert!(self.len() >= HEADER_LEN);
        self.total_body_len() as usize + HEADER_LEN
    }
    #[inline(always)]
    fn status_ok(&self) -> bool {
        debug_assert!(self.len() >= HEADER_LEN);
        debug_assert_eq!(self.at(PacketPos::Magic as usize), RESPONSE_MAGIC);
        self.at(6) == 0 && self.at(7) == 0
    }
    #[inline(always)]
    fn key_len(&self) -> u16 {
        debug_assert!(self.len() >= HEADER_LEN);
        self.read_u16(PacketPos::Key as usize)
    }
    #[inline(always)]
    fn key(&self) -> Self {
        debug_assert!(self.len() >= HEADER_LEN);
        let extra_len = self.extra_len() as usize;
        let offset = extra_len + HEADER_LEN;
        let key_len = self.key_len() as usize;
        debug_assert!(key_len + offset <= self.len());
        self.sub_slice(offset, key_len)
    }
    // 仅仅用于获取value长度，注意区分total body len
    #[inline]
    fn value_len(&self) -> u32 {
        let total_body_len = self.total_body_len();
        let extra_len = self.extra_len() as u32;
        let key_len = self.key_len() as u32;
        total_body_len - extra_len - key_len
    }
    #[inline]
    fn value(&self) -> Self {
        debug_assert!(self.len() >= self.packet_len());
        let total_body_len = self.total_body_len() as usize;
        let extra_len = self.extra_len() as usize;
        let key_len = self.key_len() as usize;
        let value_len = total_body_len - extra_len - key_len;
        let offset = HEADER_LEN + extra_len + key_len;

        self.sub_slice(offset, value_len)
    }
    // 需要应对gek个各种姿势： getkq...getkq + noop, getkq...getkq + getk，对于quite cmd，肯定是multiget的非结尾请求
    #[inline(always)]
    fn quiet_get(&self) -> bool {
        is_quiet_get(self.op())
        //QUITE_GET_TABLE[self.op() as usize] == 1
    }
    #[inline(always)]
    fn clear_cas(&mut self) {
        debug_assert!(self.len() >= HEADER_LEN);
        for i in PacketPos::Cas as usize..PacketPos::Cas as usize + CAS_LEN {
            self.update(i, 0);
        }
    }
    #[inline(always)]
    fn is_quiet(&self) -> bool {
        let op = self.op();
        NOREPLY_MAPPING[op as usize] == op
    }
    // 在请求时，部分场景下把op_code进行一次映射。
    // 1. quite get请求映射成 non-quite get请求。
    //      getq(0x09) => get(0x00); getkq(0x0d) => getk(0x0c); 以实现multiget的pipeline
    // 2. 把gets(0x48), getsq(0x49) => get(0x00)请求。 // 支持gets请求只发送给master
    #[inline(always)]
    fn map_op(&mut self) -> u8 {
        let old = self.op();
        //let new = OPS_MAPPING_TABLE[old as usize];
        let new = match old {
            OP_GETQ | OP_GETS | OP_GETSQ => OP_GET,
            OP_GETKQ => OP_GETK,
            o => o,
        };
        if new != old {
            self.update(PacketPos::Opcode as usize, new);
        }
        old
    }
    #[inline(always)]
    fn map_op_noreply(&mut self) -> u8 {
        let op = self.op();
        self.update(PacketPos::Opcode as usize, NOREPLY_MAPPING[op as usize]);
        op
    }
    #[inline(always)]
    fn restore_op(&mut self, op: u8) {
        if self.op() != op {
            self.update(PacketPos::Opcode as usize, op);
        }
    }
    #[inline(always)]
    fn hash<H: sharding::hash::Hash>(&self, alg: &H) -> i64 {
        let key = self.key();
        if key.len() > 0 {
            alg.hash(&key)
        } else {
            // 这些请求都是noforward请求，不会发送到backend
            debug_assert!(self.operation().is_meta() || self.noop());
            0
        }
    }
    #[inline(always)]
    fn check_request(&self) -> Result<()> {
        debug_assert_ne!(self.len(), 0);
        if self.at(0) == REQUEST_MAGIC {
            Ok(())
        } else {
            Err(Error::RequestInvalidMagic)
        }
    }
    #[inline(always)]
    fn check_response(&self) -> Result<()> {
        debug_assert_ne!(self.len(), 0);
        if self.at(0) == RESPONSE_MAGIC {
            Ok(())
        } else {
            Err(Error::ResponseInvalidMagic)
        }
    }

    #[inline(always)]
    fn try_next_type(&self) -> TryNextType {
        let op = self.op() as usize;
        assert!(op < TRY_NEXT_TABLE.len());

        let try_next = TRY_NEXT_TABLE[op];
        if try_next != TryNextType::Unkown as u8 {
            return TryNextType::from(try_next);
        }

        // 只有set、setq 才会是unknown，此时只需要对cas再设置为NotTryNext即可
        if self.cas() > 0 {
            log::debug!("not try next for cas");
            return TryNextType::NotTryNext;
        }
        return TryNextType::from(try_next);
    }

    #[inline(always)]
    fn sentonly(&self) -> bool {
        let op = self.op();
        NOREPLY_MAPPING[op as usize] == op
    }
    #[inline(always)]
    fn noforward(&self) -> bool {
        //NO_FORWARD_OPS[self.op() as usize] == 1
        match self.op() {
            OP_NOOP | OP_QUIT | OP_QUITQ | OP_VERSION | OP_STAT => true,
            _ => false,
        }
    }
}
