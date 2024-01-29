use crate::{redis::command::CommandHasher, OpCode, Operation, Result};

// 指令参数需要配合实际请求的token数进行调整，所以外部使用都通过方法获取
#[derive(Default, Debug)]
pub struct CommandProperties {
    pub(crate) name: &'static str,
    pub(crate) op_code: OpCode,
    pub(crate) cmd_type: CommandType,
    // cmd 参数的个数，对于不确定的cmd，如mget、mset用负数表示最小数量
    arity: i8,
    /// cmd的类型
    pub(crate) op: Operation,
    pub(crate) supported: bool,
    pub(crate) has_key: bool,                  // 是否有key
    pub(crate) can_hold_field: bool,           //能否持有field
    pub(crate) can_hold_where_condition: bool, // 能否持有where condition
    // 指令在不路由或者无server响应时的响应位置，
    pub(crate) padding_rsp: &'static str,
    pub(crate) noforward: bool,
    pub(crate) quit: bool, // 是否需要quit掉连接
}

// 默认响应
// 第0个表示quit
const PADDING_RSP_TABLE: [&str; 5] = [
    "",
    "+OK\r\n",
    "+PONG\r\n",
    "-ERR kvector no available\r\n",
    "-ERR invalid command\r\n",
];

impl CommandProperties {
    // mesh 需要进行validate，避免不必要的异常 甚至 hang住 fishermen
    #[inline]
    pub fn validate(&self, total_bulks: usize) -> Result<()> {
        // 初始化时会进行check arity，此处主要是心理安慰剂，另外避免init的arity check被不小心干掉
        debug_assert!(self.arity != 0, "redis cmd:{}", self.name);

        if self.arity > 0 {
            // 如果cmd的arity大于0，请求参数必须等于cmd的arity
            if total_bulks == (self.arity as usize) {
                return Ok(());
            }
        } else if total_bulks >= (self.arity.abs() as usize) {
            // 如果cmd的arity小于0，请求参数必须大于等于cmd的arity绝对值
            return Ok(());
        }

        use super::error::KvectorError;
        Err(KvectorError::ReqInvalidBulkNum.into())
    }

    pub(crate) fn flag(&self) -> crate::Flag {
        let mut flag = crate::Flag::from_op(self.op_code, self.op);
        flag.set_noforward(self.noforward);
        flag
    }
}

pub(super) struct Commands {
    supported: [CommandProperties; Self::MAPPING_RANGE],
    // hash: Crc32,
    //hash: Bkdr,
}
impl Commands {
    /// 指令不多，2048应该够了
    const MAPPING_RANGE: usize = 2048;
    fn new() -> Self {
        Self {
            supported: array_init::array_init(|_| Default::default()),
            // hash: Crc32::default(),
            //hash: Bkdr::default(),
        }
    }

    #[inline]
    pub(crate) fn get_by_op(&self, op_code: u16) -> crate::Result<&CommandProperties> {
        assert!((op_code as usize) < self.supported.len(), "op:{}", op_code);
        let cmd = unsafe { self.supported.get_unchecked(op_code as usize) };
        if cmd.supported {
            Ok(cmd)
        } else {
            Err(super::error::KvectorError::ReqNotSupported.into())
        }
    }

    // #[inline]
    // pub(crate) fn get_by_name(&self, name: &str) -> crate::Result<&CommandProperties> {
    //     for cmd in self.supported.iter() {
    //         if cmd.name == name {
    //             return Ok(cmd);
    //         }
    //     }
    //     Err(super::error::KvectorError::ReqNotSupported.into())
    // }

    #[inline]
    fn add_support(&mut self, mut c: CommandProperties) {
        let idx = CommandHasher::hash_bytes(c.name.as_bytes()) as usize;
        assert!(idx > 0 && idx < self.supported.len(), "idx:{}", idx);
        // 之前没有添加过。
        assert!(!self.supported[idx].supported);
        c.supported = true;
        c.op_code = idx as u16;

        // arity 不能为0
        assert!(c.arity != 0, "invalid redis cmd: {}", c.name);

        self.supported[idx] = c;
    }
}

#[inline(always)]
pub fn get_cfg(op_code: u16) -> crate::Result<&'static CommandProperties> {
    SUPPORTED.get_by_op(op_code)
}

// #[inline(always)]
// pub(crate) fn get_cfg_byname(name: &str) -> crate::Result<&'static CommandProperties> {
//     SUPPORTED.get_by_name(name)
// }

use Operation::*;
type Cmd = CommandProperties;
#[ctor::ctor]
#[rustfmt::skip]
pub(super) static SUPPORTED: Commands = {
    let mut cmds = Commands::new();
    let pt = PADDING_RSP_TABLE;
    // TODO：后续增加新指令时，当multi/need_bulk_num 均为true时，需要在add_support中进行nil转换，避免将err返回到client fishermen
    for c in vec![
        //// meta 指令
        //// 不支持select 0以外的请求。所有的select请求直接返回，默认使用db0
        //// hello 参数应该是-1，可以不带或者带多个
        Cmd::new("command").arity(-1).op(Meta).padding(pt[1]).nofwd(),
        Cmd::new("ping").arity(-1).op(Meta).padding(pt[2]).nofwd(),
        Cmd::new("select").arity(2).op(Meta).padding(pt[1]).nofwd(),
        Cmd::new("hello").arity(-1).op(Meta).padding(pt[4]).nofwd(),
        // quit、master的指令token数/arity应该都是1,quit 的padding设为1 
        Cmd::new("quit").arity(1).op(Meta).padding(pt[1]).nofwd().quit(),

        // kvector 相关的指令
        Cmd::new("vget").arity(-2).op(Get).cmd_type(CommandType::VGet).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),
        Cmd::new("vrange").arity(-2).op(Get).cmd_type(CommandType::VRange).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),
        Cmd::new("vadd").arity(-2).op(Store).cmd_type(CommandType::VAdd).padding(pt[3]).has_key().can_hold_field(),
        // Cmd::new("vreplace").arity(-2).op(Store).cmd_type(CommandType::VReplace).padding(pt[3]).has_key().can_hold_field(),
        Cmd::new("vupdate").arity(-2).op(Store).cmd_type(CommandType::VUpdate).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),
        Cmd::new("vdel").arity(-2).op(Store).cmd_type(CommandType::VDel).padding(pt[3]).has_key().can_hold_where_condition(),
        Cmd::new("vcard").arity(-2).op(Get).cmd_type(CommandType::VCard).padding(pt[3]).has_key().can_hold_where_condition(),
    ] {
        cmds.add_support(c);
    }
    cmds
};

impl CommandProperties {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            ..Default::default()
        }
    }
    pub(crate) fn arity(mut self, arity: i8) -> Self {
        self.arity = arity;
        self
    }
    pub(crate) fn op(mut self, op: Operation) -> Self {
        self.op = op;
        self
    }
    pub(crate) fn cmd_type(mut self, cmd_type: CommandType) -> Self {
        self.cmd_type = cmd_type;
        self
    }
    pub(crate) fn padding(mut self, padding_rsp: &'static str) -> Self {
        self.padding_rsp = padding_rsp;
        self
    }

    pub(crate) fn nofwd(mut self) -> Self {
        self.noforward = true;
        self
    }
    pub(crate) fn has_key(mut self) -> Self {
        self.has_key = true;
        self
    }
    pub(crate) fn can_hold_field(mut self) -> Self {
        self.can_hold_field = true;
        self
    }
    pub(crate) fn can_hold_where_condition(mut self) -> Self {
        self.can_hold_where_condition = true;
        self
    }
    pub(crate) fn quit(mut self) -> Self {
        self.quit = true;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum CommandType {
    // kvector 访问的指令
    VGet,
    VRange,
    VAdd,
    // VReplace,
    VUpdate,
    VDel,
    VCard,

    // // 兼容redisclient而引入的指令
    // Select,
    // Ping,
    // Hello,
    // Quit,

    // 未知or不支持的指令
    Unknown,
}
impl Default for CommandType {
    fn default() -> Self {
        CommandType::Unknown
    }
}
// impl From<RingSlice> for CommandType {
//     fn from(name: RingSlice) -> Self {
//         if name.len() >= 7 {
//             return Self::Unknown;
//         }

//         let mut oft = 0;
//         // 第一个字符不是V，cmd就是扩展的兼容指令 or 未知指令
//         if name.scan_to_uppercase(&mut oft) == b'V' {
//             match name.scan_to_uppercase(&mut oft) {
//                 b'R' => Self::parse_to_cmd(&name, oft, "VRANGE", Self::VRange),
//                 b'A' => Self::parse_to_cmd(&name, oft, "VADD", Self::VAdd),
//                 b'U' => Self::parse_to_cmd(&name, oft, "VUPDATE", Self::VUpdate),
//                 b'D' => Self::parse_to_cmd(&name, oft, "VDEL", Self::VDel),
//                 b'C' => Self::parse_to_cmd(&name, oft, "VCARD", Self::VCard),
//                 _ => Self::Unknown,
//             }
//         } else {
//             match name.scan_to_uppercase(&mut oft) {
//                 b'S' => Self::parse_to_cmd(&name, oft, "SELECT", Self::Select),
//                 b'P' => Self::parse_to_cmd(&name, oft, "PING", Self::Ping),
//                 b'H' => Self::parse_to_cmd(&name, oft, "HELLO", Self::Hello),
//                 b'Q' => Self::parse_to_cmd(&name, oft, "QUIT", Self::Quit),
//                 _ => Self::Unknown,
//             }
//         }
//     }
// }

// impl CommandType {
//     /// 检测oft之后的name是否于对应cmd name相同，如果相同则返回对应的CMD
//     #[inline]
//     fn parse_to_cmd(name: &RingSlice, oft: usize, cmd: &str, cmd_type: CommandType) -> Self {
//         // 检测oft位置目前只有1和2，1表示非‘V’开头的redis兼容指令，2表示V开头的kvector指令
//         assert!(oft == 1 || oft == 2, "{}", name);

//         if name.len() == cmd.len() && name.start_ignore_case(oft, cmd[oft..].as_bytes()) {
//             cmd_type
//         } else {
//             Self::Unknown
//         }
//     }

//     #[inline]
//     pub(super) fn operation(&self) -> Operation {
//         match self {
//             CommandType::VRange => Operation::Gets,

//             CommandType::Unknown => panic!("no operation for unknow!"),
//             _ => Operation::Store,
//         }
//     }

//     /// 这个type是否是合法的，unknow不合法
//     #[inline]
//     pub(super) fn is_invalid(&self) -> bool {
//         match self {
//             CommandType::Unknown => true,
//             _ => false,
//         }
//     }
// }

// impl Default for CommandType {
//     fn default() -> Self {
//         Self::Unknown
//     }
// }

// /// 扫描对应位置的子节，将对应位置的字符转为大写，同时后移读取位置oft
// pub trait Uppercase {
//     // 扫描当前子节，转成大写，并讲位置+1
//     fn scan_to_uppercase(&self, oft: &mut usize) -> u8;
// }

// impl Uppercase for RingSlice {
//     fn scan_to_uppercase(&self, oft: &mut usize) -> u8 {
//         let b = self.at(*oft);
//         *oft += 1;
//         b.to_ascii_uppercase()
//     }
// }
