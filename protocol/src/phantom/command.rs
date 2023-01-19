use crate::{HashedCommand, OpCode, Operation, Result};
use ds::{MemGuard, RingSlice};
use sharding::hash::{Crc32, Hash, UppercaseHashKey};

// 指令参数需要配合实际请求的token数进行调整，所以外部使用都通过方法获取
#[allow(dead_code)]
#[derive(Default, Clone, Copy, Debug)]
pub(crate) struct CommandProperties {
    pub(super) name: &'static str,
    pub(super) mname: &'static str, // 将multi key映射成单个key的get命令，发送到backend
    pub(super) op_code: OpCode,
    // cmd 参数的个数，对于不确定的cmd，如mget、mset用负数表示最小数量
    pub(super) arity: i8,
    /// cmd的类型
    pub(super) op: Operation,
    /// 第一个key所在的位置
    first_key_index: u8,
    /// 最后一个key所在的位置，注意对于multi-key cmd，用负数表示相对位置
    last_key_index: i8,
    /// key 步长，get的步长为1，mset的步长为2，like:k1 v1 k2 v2
    key_step: u8,
    // 指令在不路由或者无server响应时的响应位置，
    pub(super) padding_rsp: usize,

    // TODO 把padding、nil整合成一个，测试完毕后清理
    // pub(super) nil_rsp: u8,
    pub(super) has_val: bool,
    pub(super) has_key: bool,
    pub(super) noforward: bool,
    pub(super) supported: bool,
    pub(super) multi: bool, // 该命令是否可能会包含多个key
    // need bulk number只对multi key请求的有意义
    pub(super) need_bulk_num: bool, // mset所有的请求只返回一个+OK，不需要在首个请求前加*bulk_num。其他的都需要
    pub(super) quit: bool,          // 是否需要quit掉连接
}

// 默认响应,第0个表示qui;
// bfmget、bfmset在key异常响应时，返回-10作为nil
pub const PADDING_RSP_TABLE: [&str; 6] = [
    "",
    "+OK\r\n",
    "+PONG\r\n",
    "-ERR phantom no available\r\n",
    "-ERR unknown command\r\n",
    ":-10\r\n",
];

#[allow(dead_code)]
impl CommandProperties {
    #[inline]
    pub fn operation(&self) -> &Operation {
        &self.op
    }

    #[inline]
    pub fn validate(&self, total_bulks: usize) -> Result<()> {
        debug_assert!(self.arity != 0, "cmd:{}", self.name);

        if self.arity > 0 {
            // 如果cmd的arity大于0，请求参数必须等于cmd的arity
            if total_bulks == (self.arity as usize) {
                return Ok(());
            }
        } else if total_bulks >= (self.arity.abs() as usize) {
            // 如果cmd的arity小于0，请求参数必须大于等于cmd的arity绝对值
            return Ok(());
        }

        Err(crate::Error::RequestProtocolInvalid("pt bulk num invalied"))
    }

    #[inline]
    pub fn first_key_index(&self) -> usize {
        self.first_key_index as usize
    }

    // 如果last key index为负数，token count加上该负数，即为key的结束idx
    #[inline]
    pub fn last_key_index(&self, token_count: usize) -> usize {
        assert!(
            token_count as i64 > self.first_key_index as i64
                && token_count as i64 > self.last_key_index as i64
        );
        if self.last_key_index >= 0 {
            return self.last_key_index as usize;
        } else {
            // 最后一个key的idx为负数，
            return (token_count as i64 + self.last_key_index as i64) as usize;
        }
    }

    pub fn key_step(&self) -> usize {
        self.key_step as usize
    }

    pub fn padding_rsp(&self) -> usize {
        self.padding_rsp
    }
    #[inline]
    pub fn noforward(&self) -> bool {
        self.noforward
    }
    #[inline]
    pub(super) fn flag(&self) -> crate::Flag {
        let mut flag = crate::Flag::from_op(self.op_code, self.op);
        //TODO 去掉padding_rsp，从req的cfg中获取，测试完毕后清理
        // flag.set_padding_rsp(self.padding_rsp);
        flag.set_noforward(self.noforward);
        flag
    }

    #[inline]
    pub(super) fn build_request_with_key(&self, hash: i64, real_key: &RingSlice) -> HashedCommand {
        use ds::Buffer;
        assert!(self.name.len() < 10, "name:{}", self.name);
        let mut cmd = Vec::with_capacity(24 + real_key.len());
        cmd.push(b'*');
        // 1个cmd, 1个key，1个value。一共3个bulk
        cmd.push((2 + self.has_val as u8) + b'0');
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(self.mname.len().to_string());
        cmd.write("\r\n");
        cmd.write(self.mname);
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(real_key.len().to_string());
        cmd.write("\r\n");
        cmd.write_slice(real_key);
        cmd.write("\r\n");

        //data.copy_to_vec(&mut cmd);
        let flag = self.flag();
        let cmd: MemGuard = MemGuard::from_vec(cmd);
        HashedCommand::new(cmd, hash, flag)
    }

    // bulk_num只有在first=true时才有意义。
    #[inline]
    pub(super) fn build_request_with_key_for_multi(
        &self,
        hash: i64,
        bulk_num: u16,
        first: bool,
        real_key: &RingSlice,
    ) -> HashedCommand {
        use ds::Buffer;
        assert!(self.name.len() < 10, "name:{}", self.name);
        let mut cmd = Vec::with_capacity(24 + real_key.len());
        cmd.push(b'*');
        // 1个cmd, 1个key，1个value。一共3个bulk
        cmd.push((2 + self.has_val as u8) + b'0');
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(self.mname.len().to_string());
        cmd.write("\r\n");
        cmd.write(self.mname);
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(real_key.len().to_string());
        cmd.write("\r\n");
        cmd.write_slice(real_key);
        cmd.write("\r\n");

        //data.copy_to_vec(&mut cmd);
        let mut flag = self.flag();
        use super::flag::RedisFlager;
        if first {
            flag.set_mkey_first();
            // mset只有一个返回值。
            // 其他的multi请求的key的数量就是bulk_num
            assert!(
                self.key_step == 1 || self.key_step == 2,
                "step:{}",
                self.key_step
            );
            let mut key_num = bulk_num;
            if self.key_step == 2 {
                key_num >>= 1;
            }
            flag.set_key_count(key_num);
        }
        let cmd: MemGuard = MemGuard::from_vec(cmd);
        HashedCommand::new(cmd, hash, flag)
    }

    // 构建一个padding rsp，用于返回默认响应或server不可用响应
    // 格式类似：1 pong； 2 -Err redis no available
    #[inline(always)]
    pub(super) fn get_padding_rsp(&self) -> &str {
        unsafe { *PADDING_RSP_TABLE.get_unchecked(self.padding_rsp) }
    }
}

// https://redis.io/commands 一共145大类命令。使用 crate::sharding::Hash::Crc32
// 算法能够完整的将其映射到0~4095这个区间。因为使用这个避免大量的match消耗。
pub(super) struct Commands {
    supported: [CommandProperties; Self::MAPPING_RANGE],
    hash: Crc32,
}
impl Commands {
    const MAPPING_RANGE: usize = 4096;
    fn new() -> Self {
        Self {
            supported: [CommandProperties::default(); Self::MAPPING_RANGE],
            hash: Crc32::default(),
        }
    }
    #[inline]
    pub(crate) fn get_op_code(&self, name: &ds::RingSlice) -> u16 {
        let uppercase = UppercaseHashKey::new(name);
        let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
        // op_code 0表示未定义。不存在
        assert_ne!(idx, 0);
        idx as u16
    }
    #[inline]
    pub(crate) fn get_by_op(&self, op_code: u16) -> crate::Result<&CommandProperties> {
        assert!((op_code as usize) < self.supported.len(), "op:{}", op_code);
        let cmd = unsafe { self.supported.get_unchecked(op_code as usize) };
        if cmd.supported {
            Ok(cmd)
        } else {
            Err(crate::Error::ProtocolNotSupported)
        }
    }
    // 不支持会返回协议错误
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn get_by_name(&self, cmd: &ds::RingSlice) -> crate::Result<&CommandProperties> {
        let uppercase = UppercaseHashKey::new(cmd);
        let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
        self.get_by_op(idx as u16)
    }
    fn add_support(
        &mut self,
        name: &'static str,
        mname: &'static str,
        arity: i8,
        op: Operation,
        first_key_index: u8,
        last_key_index: i8,
        key_step: u8,
        padding_rsp: usize,
        multi: bool,
        noforward: bool,
        has_key: bool,
        has_val: bool,
        need_bulk_num: bool,
    ) {
        let uppercase = name.to_uppercase();
        let idx = self.hash.hash(&uppercase.as_bytes()) as usize & (Self::MAPPING_RANGE - 1);
        assert!(idx < self.supported.len(), "idx:{}", idx);
        // 之前没有添加过。
        assert!(!self.supported[idx].supported);

        // TODO 测试完毕后清理
        // bfmget、bfmset，在部分key 返回异常响应时，需要将异常信息转换为nil返回 fishermen
        // let mut nil_rsp = 0;
        // if uppercase.eq("BFMGET") || uppercase.eq("BFMSET") {
        //     nil_rsp = 5;
        // }

        // 所有cmd的padding-rsp都必须是合理值，此处统一判断
        assert!(padding_rsp < PADDING_RSP_TABLE.len(), "cmd:{}", name);
        // arity 不能为0
        assert!(arity != 0, "invalid redis cmd: {}", name);

        let quit = uppercase.eq("QUIT");
        self.supported[idx] = CommandProperties {
            name,
            mname,
            op_code: idx as u16,
            arity,
            op,
            first_key_index,
            last_key_index,
            key_step,
            padding_rsp,
            // nil_rsp,
            noforward,
            supported: true,
            multi,
            has_key,
            has_val,
            need_bulk_num,
            quit,
        };
    }
}

#[inline]
pub(super) fn get_op_code(cmd: &ds::RingSlice) -> u16 {
    SUPPORTED.get_op_code(cmd)
}
#[inline]
pub(super) fn get_cfg<'a>(op_code: u16) -> crate::Result<&'a CommandProperties> {
    SUPPORTED.get_by_op(op_code)
}
use cmd::SUPPORTED;
pub(super) mod cmd {
    use super::Commands;
    use super::Operation::*;
    #[ctor::ctor]
    #[rustfmt::skip]
   pub(super) static SUPPORTED: Commands = {
        let mut cmds = Commands::new();
    for (name, mname, arity, op, first_key_index, last_key_index, key_step, padding_rsp, multi, noforward, has_key, has_val, need_bulk_num)
        in vec![
                // meta 指令
                ("command", "command" ,   -1, Meta, 0, 0, 0, 1, false, true, false, false, false),
                ("ping", "ping" ,         -1, Meta, 0, 0, 0, 2, false, true, false, false, false),
                // 不支持select 0以外的请求。所有的select请求直接返回，默认使用db0
                ("select", "select" ,      2, Meta, 0, 0, 0, 1, false, true, false, false, false),
                ("hello", "hello" ,        2, Meta, 0, 0, 0, 4, false, true, false, false, false),
                // quit 的padding应该为1，返回+OK，并断连接
                ("quit", "quit" ,          2, Meta, 0, 0, 0, 1, false, true, false, false, false),

                ("bfget" , "bfget",        2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("bfset", "bfset",         2, Store, 1, 1, 1, 3, false, false, true, false, false),

                // bfmget、bfmset，padding改为5， fishermen
                ("bfmget", "bfget",       -2, MGet, 1, -1, 1, 5, true, false, true, false, true),
                ("bfmset", "bfset",       -2, Store, 1, 1, 1, 5, true, false, true, false, true),



            ] {
    cmds.add_support(
        name,
        mname,
        arity,
        op,
        first_key_index,
        last_key_index,
        key_step,
        padding_rsp,
        multi,
        noforward,
        has_key,
        has_val,
        need_bulk_num
    ) ;
            }
        cmds
    };
}
