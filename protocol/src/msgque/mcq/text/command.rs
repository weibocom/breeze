use sharding::hash::{Bkdr, Hash, UppercaseHashKey};

use crate::{OpCode, Operation};

#[derive(Clone, Copy, Debug)]
pub(crate) struct CommandProperties {
    name: &'static str,
    req_type: RequestType,
    op_code: OpCode,
    op: Operation,
    padding_rsp: u8,
    noforward: bool,
    supported: bool,
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
    #[inline]
    pub fn padding_rsp(&self) -> &str {
        assert!((self.padding_rsp as usize) < PADDING_RSP_TABLE.len());
        PADDING_RSP_TABLE.get(self.padding_rsp as usize).unwrap()
    }
    #[inline]
    pub fn name(&self) -> &str {
        self.name
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
        assert!((op_code as usize) < self.supported.len());
        let cmd = unsafe { self.supported.get_unchecked(op_code as usize) };
        if cmd.supported {
            Ok(cmd)
        } else {
            Err(crate::Error::CommandNotSupported)
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
        padding_rsp: u8,
        noforward: bool,
    ) {
        let uppercase = name.to_uppercase();
        let idx = self.hash.hash(&uppercase.as_bytes()) as usize & (Self::MAPPING_RANGE - 1);
        assert!(idx < self.supported.len());
        // cmd 不能重复初始化
        assert!(!self.supported[idx].supported);

        self.supported[idx] = CommandProperties {
            name,
            req_type,
            op_code: idx as u16,
            op,
            padding_rsp,
            noforward,
            supported: true,
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
            ("delete", RequestType::Delete, Store, 1, false),
            ("stats", RequestType::Stats, Meta, 3, true),
            ("version", RequestType::Version, Meta, 2, true),
            ("quit", RequestType::Quit, Meta, 0, true),
        ] {
            cmds.add_support(name, req_type, op, padding_rsp, noforward);
        }
        cmds
    };
}

// mcq2 目前只支持如下指令
#[derive(Debug, Clone, Copy)]
pub(crate) enum RequestType {
    Unknown,
    Get,
    Set,
    Delete,
    Version,
    Stats,
    Quit,
}

impl RequestType {
    // 后续可能会有更多指令支持
    #[inline]
    pub fn is_storage(&self) -> bool {
        match self {
            RequestType::Set => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_delete(&self) -> bool {
        match self {
            RequestType::Delete => true,
            _ => false,
        }
    }

    // 后续可能会有更多指令支持
    #[inline]
    pub fn is_retrieval(&self) -> bool {
        match self {
            RequestType::Get => true,
            _ => false,
        }
    }
}
