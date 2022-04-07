use crate::Operation::{self, Get, Gets, Meta, Store};
use crate::{Error, Result};
// 低4位是command idx: Get, MGet, Gets, Store, Meta四类命令，索引分别是0-4. 0x48是扩展的Gets请求
const SENT_ONLY: u8 = 1 << 4; // 上行请求，并且是quiet
const QUIET: u8 = 1 << 5; // quiet请求
const QUITE_GET: u8 = 1 << 6; // quiet get请求
const NOFORWARD: u8 = 1 << 7; // noforward

const NOT_SUPPORTED: u8 = 0xff;

#[inline]
pub(super) fn get_op_cfg(op: u8) -> OpCfg {
    OpCfg(OP_TABLES[op as usize])
}

#[inline]
pub(super) fn get_cmd(op: u8) -> Operation {
    get_op_cfg(op).cmd().into()
}

pub(super) struct OpCfg(u8);
impl OpCfg {
    #[inline]
    pub(super) fn cmd(&self) -> u8 {
        self.0 & 0xf
    }
    #[inline]
    pub(super) fn quiet(&self) -> bool {
        self.0 & QUIET == QUIET
    }
    #[inline]
    pub(super) fn quiet_get(&self) -> bool {
        self.0 & QUITE_GET == QUITE_GET
    }
    #[inline]
    pub(super) fn sentonly(&self) -> bool {
        self.0 & SENT_ONLY == SENT_ONLY
    }
    #[inline]
    pub(super) fn noforward(&self) -> bool {
        self.0 & NOFORWARD == NOFORWARD
    }
    #[inline]
    pub(super) fn check(&self) -> Result<()> {
        if self.0 != NOT_SUPPORTED {
            Ok(())
        } else {
            Err(Error::RequestProtocolNotValid)
        }
    }
}

const OP_TABLES: [u8; 256] = [
    Get as u8,                       // 0x00 Get
    Store as u8,                     // 0x01 Set
    Store as u8,                     // 0x02 Add
    Store as u8,                     // 0x03 Replace
    Store as u8,                     // 0x04 Delete
    Store as u8,                     // 0x05 Increment
    Store as u8,                     // 0x06 Decrement
    Meta as u8 | NOFORWARD,          // 0x07 Quit
    Meta as u8 | NOFORWARD,          // 0x08 Flush
    Get as u8 | QUIET | QUITE_GET,   // 0x09 GetQ
    Get as u8 | NOFORWARD,           // 0x0a No-op
    Get as u8 | NOFORWARD,           // 0x0b Version
    Get as u8,                       // 0x0c GetK
    Get as u8 | QUIET | QUITE_GET,   // 0x0d GetKQ
    Store as u8,                     // 0x0e Append
    Store as u8,                     // 0x0f Prepend
    Meta as u8 | NOFORWARD,          // 0x10 Stat
    Store as u8 | QUIET | SENT_ONLY, // 0x11 SetQ
    Store as u8 | QUIET | SENT_ONLY, // 0x12 AddQ
    Store as u8 | QUIET | SENT_ONLY, // 0x13 ReplaceQ
    Store as u8 | QUIET | SENT_ONLY, // 0x14 DeleteQ
    Store as u8 | QUIET | SENT_ONLY, // 0x15 IncrementQ
    Store as u8 | QUIET | SENT_ONLY, // 0x16 DecrementQ
    Meta as u8 | QUIET | NOFORWARD,  // 0x17 QuitQ
    Meta as u8 | QUIET | NOFORWARD,  // 0x18 FlushQ
    Store as u8 | QUIET | SENT_ONLY, // 0x19 AppendQ
    Store as u8 | QUIET | SENT_ONLY, // 0x1a PrependQ
    Meta as u8 | NOFORWARD,          // 0x1b PrependQ
    Store as u8,                     // 0x1c Touch
    Store as u8,                     // 0x1d GAT
    Store as u8 | QUIET | SENT_ONLY, // 0x1e GATQ
    NOT_SUPPORTED,                   // 0x1f
    NOT_SUPPORTED,                   // 0x20
    NOT_SUPPORTED,                   // 0x21
    NOT_SUPPORTED,                   // 0x22
    NOT_SUPPORTED,                   // 0x23
    NOT_SUPPORTED,                   // 0x24
    NOT_SUPPORTED,                   // 0x25
    NOT_SUPPORTED,                   // 0x26
    NOT_SUPPORTED,                   // 0x27
    NOT_SUPPORTED,                   // 0x28
    NOT_SUPPORTED,                   // 0x29
    NOT_SUPPORTED,                   // 0x2a
    NOT_SUPPORTED,                   // 0x2b
    NOT_SUPPORTED,                   // 0x2c
    NOT_SUPPORTED,                   // 0x2d
    NOT_SUPPORTED,                   // 0x2e
    NOT_SUPPORTED,                   // 0x2f
    NOT_SUPPORTED,                   // 0x30
    NOT_SUPPORTED,                   // 0x31
    NOT_SUPPORTED,                   // 0x32
    NOT_SUPPORTED,                   // 0x33
    NOT_SUPPORTED,                   // 0x34
    NOT_SUPPORTED,                   // 0x35
    NOT_SUPPORTED,                   // 0x36
    NOT_SUPPORTED,                   // 0x37
    NOT_SUPPORTED,                   // 0x38
    NOT_SUPPORTED,                   // 0x39
    NOT_SUPPORTED,                   // 0x3a
    NOT_SUPPORTED,                   // 0x3b
    NOT_SUPPORTED,                   // 0x3c
    NOT_SUPPORTED,                   // 0x3d
    NOT_SUPPORTED,                   // 0x3e
    NOT_SUPPORTED,                   // 0x3f
    NOT_SUPPORTED,                   // 0x40
    NOT_SUPPORTED,                   // 0x41
    NOT_SUPPORTED,                   // 0x42
    NOT_SUPPORTED,                   // 0x43
    NOT_SUPPORTED,                   // 0x44
    NOT_SUPPORTED,                   // 0x45
    NOT_SUPPORTED,                   // 0x46
    NOT_SUPPORTED,                   // 0x47
    Gets as u8,                      // 0x48 Gets 扩展的请求
    Gets as u8 | QUIET | QUITE_GET,  // 0x49 GetsQ 扩展的请求
    NOT_SUPPORTED,                   // 0x4a
    NOT_SUPPORTED,                   // 0x4b
    NOT_SUPPORTED,                   // 0x4c
    NOT_SUPPORTED,                   // 0x4d
    NOT_SUPPORTED,                   // 0x4e
    NOT_SUPPORTED,                   // 0x4f
    NOT_SUPPORTED,                   // 0x50
    NOT_SUPPORTED,                   // 0x51
    NOT_SUPPORTED,                   // 0x52
    NOT_SUPPORTED,                   // 0x53
    NOT_SUPPORTED,                   // 0x54
    NOT_SUPPORTED,                   // 0x55
    NOT_SUPPORTED,                   // 0x56
    NOT_SUPPORTED,                   // 0x57
    NOT_SUPPORTED,                   // 0x58
    NOT_SUPPORTED,                   // 0x59
    NOT_SUPPORTED,                   // 0x5a
    NOT_SUPPORTED,                   // 0x5b
    NOT_SUPPORTED,                   // 0x5c
    NOT_SUPPORTED,                   // 0x5d
    NOT_SUPPORTED,                   // 0x5e
    NOT_SUPPORTED,                   // 0x5f
    NOT_SUPPORTED,                   // 0x60
    NOT_SUPPORTED,                   // 0x61
    NOT_SUPPORTED,                   // 0x62
    NOT_SUPPORTED,                   // 0x63
    NOT_SUPPORTED,                   // 0x64
    NOT_SUPPORTED,                   // 0x65
    NOT_SUPPORTED,                   // 0x66
    NOT_SUPPORTED,                   // 0x67
    NOT_SUPPORTED,                   // 0x68
    NOT_SUPPORTED,                   // 0x69
    NOT_SUPPORTED,                   // 0x6a
    NOT_SUPPORTED,                   // 0x6b
    NOT_SUPPORTED,                   // 0x6c
    NOT_SUPPORTED,                   // 0x6d
    NOT_SUPPORTED,                   // 0x6e
    NOT_SUPPORTED,                   // 0x6f
    NOT_SUPPORTED,                   // 0x70
    NOT_SUPPORTED,                   // 0x71
    NOT_SUPPORTED,                   // 0x72
    NOT_SUPPORTED,                   // 0x73
    NOT_SUPPORTED,                   // 0x74
    NOT_SUPPORTED,                   // 0x75
    NOT_SUPPORTED,                   // 0x76
    NOT_SUPPORTED,                   // 0x77
    NOT_SUPPORTED,                   // 0x78
    NOT_SUPPORTED,                   // 0x79
    NOT_SUPPORTED,                   // 0x7a
    NOT_SUPPORTED,                   // 0x7b
    NOT_SUPPORTED,                   // 0x7c
    NOT_SUPPORTED,                   // 0x7d
    NOT_SUPPORTED,                   // 0x7e
    NOT_SUPPORTED,                   // 0x7f
    NOT_SUPPORTED,                   // 0x80
    NOT_SUPPORTED,                   // 0x81
    NOT_SUPPORTED,                   // 0x82
    NOT_SUPPORTED,                   // 0x83
    NOT_SUPPORTED,                   // 0x84
    NOT_SUPPORTED,                   // 0x85
    NOT_SUPPORTED,                   // 0x86
    NOT_SUPPORTED,                   // 0x87
    NOT_SUPPORTED,                   // 0x88
    NOT_SUPPORTED,                   // 0x89
    NOT_SUPPORTED,                   // 0x8a
    NOT_SUPPORTED,                   // 0x8b
    NOT_SUPPORTED,                   // 0x8c
    NOT_SUPPORTED,                   // 0x8d
    NOT_SUPPORTED,                   // 0x8e
    NOT_SUPPORTED,                   // 0x8f
    NOT_SUPPORTED,                   // 0x90
    NOT_SUPPORTED,                   // 0x91
    NOT_SUPPORTED,                   // 0x92
    NOT_SUPPORTED,                   // 0x93
    NOT_SUPPORTED,                   // 0x94
    NOT_SUPPORTED,                   // 0x95
    NOT_SUPPORTED,                   // 0x96
    NOT_SUPPORTED,                   // 0x97
    NOT_SUPPORTED,                   // 0x98
    NOT_SUPPORTED,                   // 0x99
    NOT_SUPPORTED,                   // 0x9a
    NOT_SUPPORTED,                   // 0x9b
    NOT_SUPPORTED,                   // 0x9c
    NOT_SUPPORTED,                   // 0x9d
    NOT_SUPPORTED,                   // 0x9e
    NOT_SUPPORTED,                   // 0x9f
    NOT_SUPPORTED,                   // 0xa0
    NOT_SUPPORTED,                   // 0xa1
    NOT_SUPPORTED,                   // 0xa2
    NOT_SUPPORTED,                   // 0xa3
    NOT_SUPPORTED,                   // 0xa4
    NOT_SUPPORTED,                   // 0xa5
    NOT_SUPPORTED,                   // 0xa6
    NOT_SUPPORTED,                   // 0xa7
    NOT_SUPPORTED,                   // 0xa8
    NOT_SUPPORTED,                   // 0xa9
    NOT_SUPPORTED,                   // 0xaa
    NOT_SUPPORTED,                   // 0xab
    NOT_SUPPORTED,                   // 0xac
    NOT_SUPPORTED,                   // 0xad
    NOT_SUPPORTED,                   // 0xae
    NOT_SUPPORTED,                   // 0xaf
    NOT_SUPPORTED,                   // 0xb0
    NOT_SUPPORTED,                   // 0xb1
    NOT_SUPPORTED,                   // 0xb2
    NOT_SUPPORTED,                   // 0xb3
    NOT_SUPPORTED,                   // 0xb4
    NOT_SUPPORTED,                   // 0xb5
    NOT_SUPPORTED,                   // 0xb6
    NOT_SUPPORTED,                   // 0xb7
    NOT_SUPPORTED,                   // 0xb8
    NOT_SUPPORTED,                   // 0xb9
    NOT_SUPPORTED,                   // 0xba
    NOT_SUPPORTED,                   // 0xbb
    NOT_SUPPORTED,                   // 0xbc
    NOT_SUPPORTED,                   // 0xbd
    NOT_SUPPORTED,                   // 0xbe
    NOT_SUPPORTED,                   // 0xbf
    NOT_SUPPORTED,                   // 0xc0
    NOT_SUPPORTED,                   // 0xc1
    NOT_SUPPORTED,                   // 0xc2
    NOT_SUPPORTED,                   // 0xc3
    NOT_SUPPORTED,                   // 0xc4
    NOT_SUPPORTED,                   // 0xc5
    NOT_SUPPORTED,                   // 0xc6
    NOT_SUPPORTED,                   // 0xc7
    NOT_SUPPORTED,                   // 0xc8
    NOT_SUPPORTED,                   // 0xc9
    NOT_SUPPORTED,                   // 0xca
    NOT_SUPPORTED,                   // 0xcb
    NOT_SUPPORTED,                   // 0xcc
    NOT_SUPPORTED,                   // 0xcd
    NOT_SUPPORTED,                   // 0xce
    NOT_SUPPORTED,                   // 0xcf
    NOT_SUPPORTED,                   // 0xd0
    NOT_SUPPORTED,                   // 0xd1
    NOT_SUPPORTED,                   // 0xd2
    NOT_SUPPORTED,                   // 0xd3
    NOT_SUPPORTED,                   // 0xd4
    NOT_SUPPORTED,                   // 0xd5
    NOT_SUPPORTED,                   // 0xd6
    NOT_SUPPORTED,                   // 0xd7
    NOT_SUPPORTED,                   // 0xd8
    NOT_SUPPORTED,                   // 0xd9
    NOT_SUPPORTED,                   // 0xda
    NOT_SUPPORTED,                   // 0xdb
    NOT_SUPPORTED,                   // 0xdc
    NOT_SUPPORTED,                   // 0xdd
    NOT_SUPPORTED,                   // 0xde
    NOT_SUPPORTED,                   // 0xdf
    NOT_SUPPORTED,                   // 0xe0
    NOT_SUPPORTED,                   // 0xe1
    NOT_SUPPORTED,                   // 0xe2
    NOT_SUPPORTED,                   // 0xe3
    NOT_SUPPORTED,                   // 0xe4
    NOT_SUPPORTED,                   // 0xe5
    NOT_SUPPORTED,                   // 0xe6
    NOT_SUPPORTED,                   // 0xe7
    NOT_SUPPORTED,                   // 0xe8
    NOT_SUPPORTED,                   // 0xe9
    NOT_SUPPORTED,                   // 0xea
    NOT_SUPPORTED,                   // 0xeb
    NOT_SUPPORTED,                   // 0xec
    NOT_SUPPORTED,                   // 0xed
    NOT_SUPPORTED,                   // 0xee
    NOT_SUPPORTED,                   // 0xef
    NOT_SUPPORTED,                   // 0xf0
    NOT_SUPPORTED,                   // 0xf1
    NOT_SUPPORTED,                   // 0xf2
    NOT_SUPPORTED,                   // 0xf3
    NOT_SUPPORTED,                   // 0xf4
    NOT_SUPPORTED,                   // 0xf5
    NOT_SUPPORTED,                   // 0xf6
    NOT_SUPPORTED,                   // 0xf7
    NOT_SUPPORTED,                   // 0xf8
    NOT_SUPPORTED,                   // 0xf9
    NOT_SUPPORTED,                   // 0xfa
    NOT_SUPPORTED,                   // 0xfb
    NOT_SUPPORTED,                   // 0xfc
    NOT_SUPPORTED,                   // 0xfd
    NOT_SUPPORTED,                   // 0xfe
    NOT_SUPPORTED,                   // 0xff
];
