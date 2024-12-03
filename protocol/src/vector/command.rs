use crate::{redis::command::CommandHasher, vector::flager::KvFlager, Flag, HashedCommand, OpCode, Operation, Result};
use ds::{Ext, MemGuard};
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
    pub(crate) multi: bool,                    // 该命令是否可能会包含多个key
    pub(crate) can_hold_field: bool,           //能否持有field
    pub(crate) can_hold_where_condition: bool, // 能否持有where condition
    // 指令在不路由或者无server响应时的响应位置，
    pub(crate) padding_rsp: &'static str,
    pub(crate) noforward: bool,
    pub(crate) quit: bool,   // 是否需要quit掉连接
    pub(crate) route: Route, // 请求路线，timeline、si？
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

    pub(crate) fn flag(&self) -> Flag {
        let mut flag = Flag::from_op(self.op_code, self.op);
        flag.set_noforward(self.noforward);
        flag
    }
    // 重建request：只需replace key即可
    #[inline]
    pub(super) fn build_request(
        &self,
        hash: i64,
        first: bool,
        mut flag: Flag,
        key: &Vec<u8>,
        data: &MemGuard,
    ) -> HashedCommand {
        use ds::Buffer;
        let mut cmd = Vec::with_capacity(data.len());
        let org_key_pos = flag.key_pos();
        cmd.write_slice(&data.sub_slice(0, org_key_pos as usize));
        // 写入key
        cmd.push(b'$');
        cmd.write(key.len().to_string());
        cmd.write("\r\n");
        cmd.write(key);
        cmd.write("\r\n");

        // 修改flag; key_pos不变
        let org_field_pos = flag.field_pos();
        let org_condition_pos = flag.condition_pos();
        *flag.ext_mut() = 0; // reset pos
        flag.set_key_pos(org_key_pos);
        if first {
            flag.set_mkey_first();
        }
        let other_start = if org_field_pos > 0 {
            org_field_pos as u32
        } else {
            org_condition_pos
        };
        if other_start > 0 {
            let oft = other_start - cmd.len() as u32;
            cmd.write_slice(&data.sub_slice(other_start as usize, data.len() - other_start as usize));
            if org_field_pos > 0 {
                flag.set_field_pos(org_field_pos - oft as u8);
            }
            if org_condition_pos > 0 {
                flag.set_condition_pos(org_condition_pos - oft);
            }
        }

        let cmd: MemGuard = MemGuard::from_vec(cmd);
        HashedCommand::new(cmd, hash, flag)
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

use super::attachment::Route;
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
        Cmd::new("vrange").arity(-2).op(Get).cmd_type(CommandType::VRange).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),
        Cmd::new("vadd").arity(-2).op(Store).cmd_type(CommandType::VAdd).padding(pt[3]).has_key().can_hold_field(),
        // Cmd::new("vreplace").arity(-2).op(Store).cmd_type(CommandType::VReplace).padding(pt[3]).has_key().can_hold_field(),
        Cmd::new("vupdate").arity(-2).op(Store).cmd_type(CommandType::VUpdate).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),
        Cmd::new("vdel").arity(-2).op(Store).cmd_type(CommandType::VDel).padding(pt[3]).has_key().multi().can_hold_where_condition(),
        Cmd::new("vcard").route(Route::Si).arity(-2).op(Get).cmd_type(CommandType::VCard).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),

        // vget 只是从timeline获取单条记录，所以route需要设置为timeline/main
        Cmd::new("vget").route(Route::TimelineOrMain).arity(-2).op(Get).cmd_type(CommandType::VGet).padding(pt[3]).has_key().multi().can_hold_field().can_hold_where_condition(),

        // 对于timeline、si后缀指令，只是中间状态，为了处理方便，不额外增加字段，仍然作为独立指令来处理
        Cmd::new("vrange.timeline").route(Route::TimelineOrMain).arity(-2).op(Get).cmd_type(CommandType::VRangeTimeline).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),
        Cmd::new("vrange.si").route(Route::Si).arity(-2).op(Get).cmd_type(CommandType::VRangeSi).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),
        Cmd::new("vadd.timeline").route(Route::TimelineOrMain).arity(-2).op(Store).cmd_type(CommandType::VAddTimeline).padding(pt[3]).has_key().can_hold_field(),
        Cmd::new("vadd.si").route(Route::Si).arity(-2).op(Store).cmd_type(CommandType::VAddSi).padding(pt[3]).has_key().can_hold_field(),
        Cmd::new("vupdate.timeline").route(Route::TimelineOrMain).arity(-2).op(Store).cmd_type(CommandType::VUpdateTimeline).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),
        // VUpdateSi, 在decr时，会用到update.si
        Cmd::new("vupdate.si").route(Route::Si).arity(-2).op(Store).cmd_type(CommandType::VUpdateSi).padding(pt[3]).has_key().can_hold_field().can_hold_where_condition(),
        Cmd::new("vdel.timeline").route(Route::TimelineOrMain).arity(-2).op(Store).cmd_type(CommandType::VDelTimeline).padding(pt[3]).has_key().can_hold_where_condition(),
        // 部分业务，仍然会del si
        Cmd::new("vdel.si").route(Route::Si).arity(-2).op(Store).cmd_type(CommandType::VDelSi).padding(pt[3]).has_key().can_hold_where_condition(),

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
    pub(crate) fn multi(mut self) -> Self {
        self.multi = true;
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
    pub(crate) fn route(mut self, route: Route) -> Self {
        self.route = route;
        self
    }
    pub(crate) fn quit(mut self) -> Self {
        self.quit = true;
        self
    }
    #[inline]
    pub fn get_route(&self) -> Route {
        self.route
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum CommandType {
    // kvector 访问的指令
    VGet,
    VRange,
    VCard,
    VAdd,
    VUpdate,
    VDel,

    // 扩展的单表指令
    VRangeTimeline,
    VRangeSi,
    VAddTimeline,
    VAddSi,
    VUpdateTimeline,
    VUpdateSi, // 注意对于si，update只是基于count的incr、decr，并不是普通意义上的直接设置为某值
    VDelTimeline,
    VDelSi,

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
