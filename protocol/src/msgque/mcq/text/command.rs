use std::fmt::Display;

use sharding::hash::{Bkdr, Hash, UppercaseHashKey};

use crate::{OpCode, Operation};

#[derive(Clone, Copy, Debug)]
pub(crate) struct CommandProperties {
    pub(crate) name: &'static str,
    req_type: RequestType,
    op_code: OpCode,
    op: Operation,
    padding_rsp: usize, // 从u8换为usize，从而在获取时不用做转换
    noforward: bool,
    supported: bool,
    pub(super) quit: bool, // 是否需要quit掉连接
}

impl CommandProperties {
    #[inline]
    pub fn operation(&self) -> &Operation {
        &self.op
    }
    #[inline]
    pub fn op_code(&self) -> OpCode {
        self.op_code
    }
    #[inline]
    pub fn noforward(&self) -> bool {
        self.noforward
    }
    #[inline]
    pub fn req_type(&self) -> &RequestType {
        &self.req_type
    }

    // 构建一个padding rsp，用于返回默认响应或server不可用响应
    // 格式类似：1 VERSION 0.0.1\r\n ;
    //          2 SERVER_ERROR mcq not available\r\n
    #[inline(always)]
    pub(super) fn get_padding_rsp(&self) -> &str {
        // 注意：quit的padding rsp为0
        unsafe { *PADDING_RSP_TABLE.get_unchecked(self.padding_rsp) }
    }
}

impl Default for CommandProperties {
    fn default() -> Self {
        Self {
            name: Default::default(),
            req_type: RequestType::Unknown,
            op_code: Default::default(),
            op: Default::default(),
            padding_rsp: Default::default(),
            noforward: Default::default(),
            supported: Default::default(),
            quit: false,
        }
    }
}

// mcq2/mcq3有若干不同的指令
pub(super) struct Commands {
    supported: [CommandProperties; Self::MAPPING_RANGE],
    hash: Bkdr,
}

impl Commands {
    const MAPPING_RANGE: usize = 512;
    #[inline]
    fn new() -> Self {
        Self {
            supported: [CommandProperties::default(); Self::MAPPING_RANGE],
            hash: Bkdr::default(),
        }
    }
    // #[inline]
    // pub(crate) fn get_op_code(&self, name: &ds::RingSlice) -> u16 {
    //     let uppercase = UppercaseHashKey::new(name);
    //     let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
    //     // op_code 0表示未定义。不存在
    //     assert_ne!(idx, 0);
    //     idx as u16
    // }
    #[inline]
    pub(crate) fn get_by_op(&self, op_code: u16) -> crate::Result<&CommandProperties> {
        assert!(
            (op_code as usize) < self.supported.len(),
            "op_code:{}",
            op_code
        );
        let cmd = unsafe { self.supported.get_unchecked(op_code as usize) };
        if cmd.supported {
            Ok(cmd)
        } else {
            Err(crate::Error::ProtocolNotSupported)
        }
    }
    // 不支持会返回协议错误
    #[inline]
    pub(crate) fn get_by_name(&self, cmd: &ds::RingSlice) -> crate::Result<&CommandProperties> {
        let uppercase = UppercaseHashKey::new(cmd);
        let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
        // op_code 0表示未定义。不存在
        assert_ne!(idx, 0);
        self.get_by_op(idx as u16)
    }

    fn add_support(
        &mut self,
        name: &'static str,
        req_type: RequestType,
        op: Operation,
        padding_rsp: usize,
        noforward: bool,
    ) {
        let uppercase = name.to_uppercase();
        let idx = self.hash.hash(&uppercase.as_bytes()) as usize & (Self::MAPPING_RANGE - 1);
        assert!(idx < self.supported.len(), "idx: {}", idx);
        // cmd 不能重复初始化
        assert!(!self.supported[idx].supported);
        debug_assert!(padding_rsp < PADDING_RSP_TABLE.len(), "{}", name);

        let quit = uppercase.eq("QUIT");
        self.supported[idx] = CommandProperties {
            name,
            req_type,
            op_code: idx as u16,
            op,
            padding_rsp,
            noforward,
            supported: true,
            quit,
        };
    }
}

#[inline]
pub(super) fn get_cfg_by_name<'a>(cmd: &ds::RingSlice) -> crate::Result<&'a CommandProperties> {
    SUPPORTED.get_by_name(cmd)
}
#[inline]
pub(super) fn get_cfg<'a>(op_code: u16) -> crate::Result<&'a CommandProperties> {
    SUPPORTED.get_by_op(op_code)
}

pub const PADDING_RSP_TABLE: [&str; 4] = [
    "",
    "SERVER_ERROR mcq not available\r\n",
    "VERSION 0.0.1\r\n",
    "STAT supported later\r\nEND\r\n",
];

lazy_static! {
    pub(super) static ref SUPPORTED: Commands = {
        let mut cmds = Commands::new();
        use Operation::*;
        for (name, req_type, op, padding_rsp, noforward) in vec![
            ("get", RequestType::Get, Get, 1, false),
            ("set", RequestType::Set, Store, 1, false),
            // TODO 对于业务，当前不允许删除队列，暂时禁止路由
            // ("delete", RequestType::Delete, Store, 1, false),
            ("delete", RequestType::Delete, Store, 3, true),
            ("stats", RequestType::Stats, Meta, 3, true),
            ("version", RequestType::Version, Meta, 2, true),
            ("quit", RequestType::Quit, Meta, 0, true),
        ] {
            cmds.add_support(name, req_type, op, padding_rsp, noforward);
        }
        cmds
    };
}

impl Display for CommandProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

// mcq2 目前只支持如下指令
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum RequestType {
    Unknown,
    Get,
    Set,
    Delete,
    Version,
    Stats,
    Quit,
}

// 暂时不用
// impl RequestType {
//     // 后续可能会有更多指令支持
//     #[inline]
//     pub fn is_storage(&self) -> bool {
//         match self {
//             RequestType::Set => true,
//             _ => false,
//         }
//     }

//     #[inline]
//     pub fn is_delete(&self) -> bool {
//         match self {
//             RequestType::Delete => true,
//             _ => false,
//         }
//     }

//     // 后续可能会有更多指令支持
//     #[inline]
//     pub fn is_retrieval(&self) -> bool {
//         match self {
//             RequestType::Get => true,
//             _ => false,
//         }
//     }
// }
