use crate::Operation;
#[allow(dead_code)]
pub(super) enum PacketPos {
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
pub(super) const HEADER_LEN: usize = 24;

pub(super) const NOOP_REQEUST: &[u8] = &[
    128, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
pub(super) const NOOP_RESPONSE: &[u8] = &[
    129, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

// https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped#command-opcodes
// MC包含Get, Gets, Store, Meta四类命令，索引分别是0-3
pub(super) const COMMAND_IDX: [u8; 128] = [
    0, 2, 2, 2, 2, 2, 2, 3, 3, 1, 1, 3, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];
// OP_CODE对应的noreply code。
// 注意：根据业务逻辑，add会转换成setq
pub(super) const NOREPLY_MAPPING: [u8; 128] = [
    0x09, 0x11, 0x11, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x09, 0x0a, 0x0b, 0x0d, 0x0d, 0x19, 0x1a,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
    0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f,
    0x30, 0x32, 0x32, 0x34, 0x34, 0x36, 0x36, 0x38, 0x38, 0x3a, 0x3a, 0x3c, 0x3c, 0x3d, 0x3e, 0x3f,
    0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x49, 0x49, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0,
];

pub(super) const REQUEST_MAGIC: u8 = 0x80;
pub(super) const RESPONSE_MAGIC: u8 = 0x81;
pub(super) const OP_CODE_GET: u8 = 0x00;
pub(super) const OP_CODE_NOOP: u8 = 0x0a;
pub(super) const OP_CODE_GETK: u8 = 0x0c;
pub(super) const OP_CODE_GETKQ: u8 = 0x0d;
pub(super) const OP_CODE_GETQ: u8 = 0x09;
// 这个专门为gets扩展
pub(super) const OP_CODE_GETS: u8 = 0x48;
pub(super) const OP_CODE_GETSQ: u8 = 0x49;

pub(super) const OP_CODE_ADD: u8 = 0x02;
pub(super) const OP_CODE_SET: u8 = 0x01;

// 0x09: getq
// 0x0d: getkq
pub(super) const MULT_GETS: [u8; 128] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

pub(super) trait Binary<T> {
    fn op(&self) -> u8;
    fn operation(&self) -> Operation;
    fn noop(&self) -> bool;
    fn request(&self) -> bool;
    fn response(&self) -> bool;
    fn extra_len(&self) -> u8;
    fn extra_or_flag(&self) -> T;
    fn total_body_len(&self) -> u32;
    fn opaque(&self) -> u32;
    fn cas(&self) -> u64;
    fn status_ok(&self) -> bool;
    fn key(&self) -> T;
    fn key_len(&self) -> u16;
    // 仅仅用于获取value长度，注意区分total body len
    fn value_len(&self) -> u32;
    fn value(&self) -> T;
    // id: 主要用来在多层请求时，用来对比request与response的key
    // 如果请求包含key，则直接获取key。
    // 如果不包含key，当前请求是get_q请求，则使用协议中的Opaque，作为key。
    // 如果是noop请求，则直接忽略.
    // 其它，panic
    fn id(&self) -> Option<T>;
    fn packet_len(&self) -> usize;
    // 是否为quite get请求。
    fn quite_get(&self) -> bool;
    // 截取末尾的noop请求
    fn take_noop(&self) -> T;

    // // 变更request的opcode
    // fn update_opcode(&self, opcode: u8);
    // // 变更request的cas
    // fn update_cas(&self, cas: u64);
}

use ds::{RingSlice, Slice};
macro_rules! define_binary {
    ($type_name:tt) => {
        impl Binary<$type_name> for $type_name {
            #[inline]
            fn op(&self) -> u8 {
                assert!(self.len() >= HEADER_LEN);
                self.at(PacketPos::Opcode as usize)
            }
            #[inline]
            fn noop(&self) -> bool {
                self.op() == OP_CODE_NOOP
            }
            #[inline]
            fn operation(&self) -> Operation {
                (COMMAND_IDX[self.op() as usize] as usize).into()
            }
            #[inline]
            fn request(&self) -> bool {
                assert!(self.len() > 0);
                self.at(PacketPos::Magic as usize) == REQUEST_MAGIC
            }
            #[inline]
            fn response(&self) -> bool {
                assert!(self.len() > 0);
                self.at(PacketPos::Magic as usize) == RESPONSE_MAGIC
            }
            #[inline]
            fn extra_len(&self) -> u8 {
                assert!(self.len() >= HEADER_LEN);
                self.at(PacketPos::ExtrasLength as usize)
            }
            fn extra_or_flag(&self) -> Self {
                // 读取flag时，需要有完整的packet
                assert!(self.len() >= HEADER_LEN);
                let extra_len = self.extra_len() as usize;
                self.sub_slice(HEADER_LEN, extra_len)
            }
            #[inline]
            fn total_body_len(&self) -> u32 {
                assert!(self.len() >= HEADER_LEN);
                self.read_u32(PacketPos::TotalBodyLength as usize)
            }
            #[inline]
            fn opaque(&self) -> u32 {
                assert!(self.len() >= HEADER_LEN);
                self.read_u32(PacketPos::Opaque as usize)
            }
            #[inline]
            fn cas(&self) -> u64 {
                assert!(self.len() >= HEADER_LEN);
                self.read_u64(PacketPos::Cas as usize)
            }
            #[inline]
            fn packet_len(&self) -> usize {
                assert!(self.len() >= HEADER_LEN);
                self.total_body_len() as usize + HEADER_LEN
            }
            #[inline]
            fn status_ok(&self) -> bool {
                assert!(self.len() >= HEADER_LEN);
                assert_eq!(self.at(PacketPos::Magic as usize), RESPONSE_MAGIC);
                self.at(6) == 0 && self.at(7) == 0
            }
            #[inline]
            fn key_len(&self) -> u16 {
                assert!(self.len() >= HEADER_LEN);
                self.read_u16(PacketPos::Key as usize)
            }
            #[inline]
            fn key(&self) -> Self {
                assert!(self.len() >= HEADER_LEN);
                let extra_len = self.extra_len() as usize;
                let offset = extra_len + HEADER_LEN;
                let key_len = self.key_len() as usize;
                assert!(key_len + offset <= self.len());
                self.sub_slice(offset, key_len)
            }
            // 仅仅用于获取value长度，注意区分total body len
            fn value_len(&self) -> u32 {
                let total_body_len = self.total_body_len();
                let extra_len = self.extra_len() as u32;
                let key_len = self.key_len() as u32;
                total_body_len - extra_len - key_len
            }
            fn value(&self) -> Self {
                assert!(self.len() >= self.packet_len());
                let total_body_len = self.total_body_len() as usize;
                let extra_len = self.extra_len() as usize;
                let key_len = self.key_len() as usize;
                let value_len = total_body_len - extra_len - key_len;
                let offset = HEADER_LEN + extra_len + key_len;

                self.sub_slice(offset, value_len)
            }
            #[inline]
            fn id(&self) -> Option<Self> {
                assert!(self.len() >= HEADER_LEN);
                match self.op() {
                    OP_CODE_NOOP => None,
                    OP_CODE_GET | OP_CODE_GETQ => Some(self.sub_slice(12, 4)),
                    _ => {
                        assert!(self.key_len() > 0);
                        Some(self.key())
                    }
                }
            }
            // 需要应对gek个各种姿势： getkq...getkq + noop, getkq...getkq + getk，对于quite cmd，肯定是multiget的非结尾请求
            #[inline]
            fn quite_get(&self) -> bool {
                MULT_GETS[self.op() as usize] == 1
            }

            // 截取末尾的noop请求
            #[inline]
            fn take_noop(&self) -> Self {
                assert!(self.len() >= HEADER_LEN);
                let noop = self.sub_slice(self.len() - HEADER_LEN, HEADER_LEN);
                assert!(noop.data() == NOOP_REQEUST || noop.data() == NOOP_RESPONSE);
                noop
            }

            // fn update_opcode(&self, opcode: u8) {
            //     assert!(self.len() >= HEADER_LEN);
            //     self.data
            // }
        }
    };
}

define_binary!(Slice);
define_binary!(RingSlice);

use crate::{Request, Response};
#[macro_export]
macro_rules! define_packet_parser {
    ($fn_name:ident, $type_in:tt, $type_out:tt, $key_ok:tt) => {
        #[inline]
        pub(super) fn $fn_name(r: &$type_in) -> Option<$type_out> {
            let mut read = 0usize;
            // 包含的是整个请求，不仅仅是key
            let mut keys: Vec<$type_in> = Vec::with_capacity(24);
            let mut c_r = r.sub_slice(0, r.len());
            while c_r.len() >= HEADER_LEN {
                let packet_len = c_r.packet_len();
                if c_r.len() < packet_len {
                    // 当前packet未读取完成
                    return None;
                }
                // 1. 非noop的request请求，或者
                // 2. 成功的response
                if !c_r.noop() && ($key_ok || c_r.status_ok()) {
                    keys.push(c_r.sub_slice(0, packet_len));
                }
                read += packet_len;
                // 把完整的命令写入进去。方便后面处理
                // getMulti的姿势On(quite-cmd) + O1(non-quite-cmd)，最后通常一个noop请求或者getk等 非quite请求 结尾
                if !c_r.quite_get() {
                    return Some($type_out::from(r.sub_slice(0, read), r.operation(), keys));
                }
                c_r = r.sub_slice(read, r.len() - read);
            }
            None
        }
    };
}

define_packet_parser!(parse_request, Slice, Request, true);
define_packet_parser!(parse_response, RingSlice, Response, false);
