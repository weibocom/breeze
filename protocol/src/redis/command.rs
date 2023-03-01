use crate::{HashedCommand, OpCode, Operation, Result};
use ds::{MemGuard, RingSlice};
//use sharding::hash::{Bkdr, Hash, HashKey, UppercaseHashKey};

#[derive(Default, Debug, PartialEq, Clone, Copy)]
pub(crate) enum CommandType {
    #[default]
    Other,
    //============== 吞噬指令 ==============//
    SwallowedMaster,
    SwallowedCmdHashkeyq,
    SwallowedCmdHashrandomq,

    CmdSendToAll,
    CmdSendToAllq,
    //============== 需要本地构建特殊响应的cmd ==============//
    // 指示下一个cmd的用于计算分片hash的key
    SpecLocalCmdHashkey,
    // 计算批量key的分片索引
    SpecLocalCmdKeyshard,
}

#[derive(Default)]
pub(super) struct CommandHasher(i32);
impl CommandHasher {
    #[inline(always)]
    fn hash(&mut self, mut b: u8) {
        if b.is_ascii_lowercase() {
            // 转换大写  32 = 'a' - 'A'
            b -= b'a' - b'A';
        }
        // 31作为seed
        self.0 = self.0.wrapping_mul(31).wrapping_add(b as i32);
    }
    #[inline(always)]
    fn finish(self) -> u16 {
        // +1 避免0
        1 + (self.0.abs() as usize & (Commands::MAPPING_RANGE - 1)) as u16
    }
    fn hash_bytes(data: &[u8]) -> u16 {
        let mut h = CommandHasher::default();
        for b in data {
            h.hash(*b);
        }
        h.finish()
    }
    // oft: 指向'\r'的位置
    #[inline(always)]
    pub(super) fn hash_slice(slice: &RingSlice, oft: usize) -> Result<(u16, usize)> {
        let mut h = CommandHasher::default();
        for i in oft..slice.len() {
            if slice[i] == b'\r' {
                return Ok((h.finish(), i));
            }
            h.hash(slice[i]);
        }
        Err(crate::Error::ProtocolIncomplete)
    }
}

// 指令参数需要配合实际请求的token数进行调整，所以外部使用都通过方法获取
#[derive(Default, Debug)]
pub(crate) struct CommandProperties {
    pub(crate) name: &'static str,
    pub(crate) mname: &'static str, // 将multi key映射成单个key的get命令，发送到backend
    pub(crate) mname_len: String,   // mname.len().to_string()
    pub(crate) op_code: OpCode,
    // cmd 参数的个数，对于不确定的cmd，如mget、mset用负数表示最小数量
    arity: i8,
    /// cmd的类型
    pub(crate) op: Operation,
    /// 第一个key所在的位置
    first_key_index: u8,
    /// 最后一个key所在的位置，注意对于multi-key cmd，用负数表示相对位置
    last_key_index: i8,
    /// key 步长，get的步长为1，mset的步长为2，like:k1 v1 k2 v2
    pub(crate) key_step: u8,
    // 指令在不路由或者无server响应时的响应位置，
    pub(crate) padding_rsp: &'static str,

    // TODO 把padding、nil、special-rsp整合成一个
    // multi类指令，如果返回多个bulk，err bulk需要转为nil
    // pub(crate) nil_rsp: u8,
    pub(crate) has_val: bool,
    pub(crate) has_key: bool,
    pub(crate) noforward: bool,
    pub(crate) supported: bool,
    pub(crate) multi: bool, // 该命令是否可能会包含多个key
    // need bulk number只对multi key请求的有意义
    pub(crate) need_bulk_num: bool, // mset所有的请求只返回一个+OK，不需要在首个请求前加*bulk_num。其他的都需要
    pub(crate) swallowed: bool, // 该指令是否需要mesh 吞噬，吞噬后不会响应client、也不会发给后端server，吞噬指令一般用于指示下一个常规指令的额外处理属性
    pub(crate) reserve_hash: bool, // 是否为下一个cmd指定hash，如果为true，将当前hash存储下来，供下一个cmd使用
    pub(crate) need_reserved_hash: bool, // 是否需要前一个指令明确指定的hash，如果为true，则必须有key或者通过hashkey指定明确的hash
    pub(crate) master_next: bool,        // 是否需要将下一个cmd发送到master
    pub(crate) quit: bool,               // 是否需要quit掉连接
    pub(crate) cmd_type: CommandType,    //用来标识自身，opcode非静态可知
}

// 默认响应
// 第0个表示quit
const PADDING_RSP_TABLE: [&str; 8] = [
    "",
    "+OK\r\n",
    "+PONG\r\n",
    "-ERR redis no available\r\n",
    "-ERR invalid command\r\n",
    "-ERR should swallowed in mesh\r\n", // 仅仅占位，会在mesh内吞噬掉，不会返回给client or server
    "$-1\r\n",                           // mget 等指令对应的nil
    ":-10\r\n",                          //phantom -1返回已被服务端占用
];

// 调用式确保idx < PADDING_RSP_TABLE.len()
// 这个idx通常来自于CommandProperties.padding_rsp

impl CommandProperties {
    // TODO 测试完毕清理 deadcode fishermen
    // 构建一个padding rsp，用于返回默认响应、server不可用响应、nil响应；
    // 响应格式类似：1 pong； 2 -Err redis no available; 3 $-1\r\n
    #[inline(always)]
    pub(crate) fn get_padding_rsp(&self) -> &str {
        self.padding_rsp
    }

    // // 构建hashkey的resp,格式:1\r\n
    // pub(super) fn get_rsp_hashkey(&self, shard: usize) -> String {
    //     format!(":{}\r\n", shard)
    // }

    // // 构建keyshard的resp，注意返回的bulk num，格式:$1\r\n1\r\n
    // pub(super) fn get_rsp_keyshard(&self, shard: usize) -> String {
    //     let shard_str = shard.to_string();
    //     format!("${}\r\n{}\r\n", shard_str.len(), shard_str)
    // }

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

        Err(crate::Error::RequestProtocolInvalid("bulk num invalied"))
    }

    // #[inline]
    // pub fn first_key_index(&self) -> usize {
    //     self.first_key_index as usize
    // }

    // 如果last key index为负数，token count加上该负数，即为key的结束idx
    // #[inline]
    // pub fn last_key_index(&self, token_count: usize) -> usize {
    //     assert!(
    //         token_count as i64 > self.first_key_index as i64
    //             && token_count as i64 > self.last_key_index as i64
    //     );
    //     if self.last_key_index >= 0 {
    //         return self.last_key_index as usize;
    //     } else {
    //         // 最后一个key的idx为负数，
    //         return (token_count as i64 + self.last_key_index as i64) as usize;
    //     }
    // }

    pub(crate) fn flag(&self) -> crate::Flag {
        let mut flag = crate::Flag::from_op(self.op_code, self.op);
        // TODO padding_rsp不再放到flag中，测试完毕后清理 fishermen
        // use super::flag::RedisFlager;
        // flag.set_padding_rsp(self.padding_rsp);
        flag.set_noforward(self.noforward);
        flag
    }

    // bulk_num只有在first=true时才有意义。
    #[inline]
    pub(super) fn build_request(
        &self,
        hash: i64,
        bulk_num: u16,
        first: bool,
        master_only: bool,
        sendto_all: bool,
        data: &RingSlice,
    ) -> HashedCommand {
        use ds::Buffer;
        //assert!(self.name.len() < 10, "name:{}", self.name);
        let mut cmd = Vec::with_capacity(16 + data.len());
        cmd.push(b'*');
        // 1个cmd, 1个key，1个value。一共3个bulk
        cmd.push((2 + self.has_val as u8) + b'0');
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(&self.mname_len);
        cmd.write("\r\n");
        cmd.write(self.mname);
        cmd.write("\r\n");
        cmd.write_slice(data);
        //data.copy_to_vec(&mut cmd);
        let mut flag = self.flag();
        use super::flag::RedisFlager;
        if first {
            flag.set_mkey_first();
            // mset只有一个返回值。
            // 其他的multi请求的key的数量就是bulk_num
            assert!(
                self.key_step == 1 || self.key_step == 2,
                "name:{}",
                self.name
            );
            let mut key_num = bulk_num;
            if self.key_step == 2 {
                key_num >>= 1;
            }
            flag.set_key_count(key_num);
        }
        if master_only {
            flag.set_master_only();
        }
        if sendto_all {
            flag.set_sendto_all()
        }
        let cmd: MemGuard = MemGuard::from_vec(cmd);
        HashedCommand::new(cmd, hash, flag)
    }
}

// https://redis.io/commands 一共145大类命令。使用 crate::sharding::Hash::Crc32
// 算法能够完整的将其映射到0~4095这个区间。因为使用这个避免大量的match消耗。
pub(super) struct Commands {
    supported: [CommandProperties; Self::MAPPING_RANGE],
    // hash: Crc32,
    //hash: Bkdr,
}
impl Commands {
    const MAPPING_RANGE: usize = 2048;
    fn new() -> Self {
        Self {
            supported: array_init::array_init(|_| Default::default()),
            // hash: Crc32::default(),
            //hash: Bkdr::default(),
        }
    }
    //#[inline]
    //pub(crate) fn get_op_code(&self, name: &ds::RingSlice) -> u16 {
    //    let uppercase = UppercaseHashKey::new(name);
    //    // let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
    //    let idx = self.inner_hash(&uppercase);
    //    // op_code 0表示未定义,不存在
    //    assert_ne!(idx, 0);
    //    idx as u16
    //}

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
    //#[inline]
    //pub(crate) fn get_by_name(&self, cmd: &ds::RingSlice) -> crate::Result<&CommandProperties> {
    //    let uppercase = UppercaseHashKey::new(cmd);
    //    // let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
    //    let idx = self.inner_hash(&uppercase);
    //    self.get_by_op(idx as u16)
    //}

    //#[inline]
    //fn inner_hash<K: HashKey>(&self, key: &K) -> usize {
    //    let idx = self.hash.hash(key) as usize & (Self::MAPPING_RANGE - 1);
    //    // 由于op_code 0表示未定义,不存在,故对于0需要进行转换为1
    //    if idx == 0 {
    //        return 1;
    //    }
    //    idx
    //}

    #[inline]
    fn add_support(&mut self, mut c: CommandProperties) {
        //use sharding::hash::Hash;
        //let old_idx = sharding::hash::Bkdr.hash(&c.name.to_uppercase().as_bytes()) as usize
        //    & (Self::MAPPING_RANGE - 1);
        let idx = CommandHasher::hash_bytes(c.name.as_bytes()) as usize;
        assert!(idx > 0 && idx < self.supported.len(), "idx:{}", idx);
        // 之前没有添加过。
        assert!(!self.supported[idx].supported);
        c.supported = true;
        c.op_code = idx as u16;

        // arity 不能为0
        assert!(c.arity != 0, "invalid redis cmd: {}", c.name);

        // 所有非swallowed cmd的padding-rsp都必须是合理值，此处统一判断
        if !c.swallowed {
            assert!(!c.padding_rsp.is_empty(), "cmd:{}", c.name);
        }
        self.supported[idx] = c;
    }
}

//#[inline]
//pub(super) fn get_op_code(cmd: &ds::RingSlice) -> u16 {
//    SUPPORTED.get_op_code(cmd)
//}
#[inline(always)]
pub(crate) fn get_cfg(op_code: u16) -> crate::Result<&'static CommandProperties> {
    SUPPORTED.get_by_op(op_code)
}

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
        // name, mname, arity, op, first_key_index, last_key_index, key_step, padding_rsp, multi, noforward, has_key, has_val, need_bulk_num
        //("command", "command" ,   -1, Meta, 0, 0, 0, 1, false, true, false, false, false),
        //("ping", "ping" ,         -1, Meta, 0, 0, 0, 2, false, true, false, false, false),
        //// 不支持select 0以外的请求。所有的select请求直接返回，默认使用db0
        //("select", "select" ,      2, Meta, 0, 0, 0, 1, false, true, false, false, false),
        //("hello", "hello" ,        -1, Meta, 0, 0, 0, 4, false, true, false, false, false),
        //("quit", "quit" ,          1, Meta, 0, 0, 0, 1, false, true, false, false, false),
        // hello 参数应该是-1，可以不带或者带多个
        Cmd::new("command").arity(-1).op(Meta).padding(pt[1]).nofwd(),
        Cmd::new("ping").arity(-1).op(Meta).padding(pt[2]).nofwd(),
        Cmd::new("select").arity(2).op(Meta).padding(pt[1]).nofwd(),
        Cmd::new("hello").arity(-1).op(Meta).padding(pt[4]).nofwd(),
        // quit、master的指令token数/arity应该都是1,quit 的padding设为1 
        // TODO quit 的padding设为1，需要验证后删除本注释 fishermen
        Cmd::new("quit").arity(1).op(Meta).padding(pt[1]).nofwd().quit(),
        Cmd::new("master").arity(1).op(Meta).nofwd().master().swallow().cmd_type(CommandType::SwallowedMaster),

        //("get" , "get",            2, Get, 1, 1, 1, 3, false, false, true, false, false),
        Cmd::new("get").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),

        // multi请求：异常响应需要改为$-1
        //("mget", "get",           -2, MGet, 1, -1, 1, 3, true, false, true, false, true),

        //("set" ,"set",             -3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("incr" ,"incr",           2, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("decr" ,"decr",           2, Store, 1, 1, 1, 3, false, false, true, false, false),
        // Cmd::new("mget").m("get").arity(-2).op(MGet).first(1).last(-1).step(1).padding(pt[3]).multi().key().bulk().nil_rsp(6),
        Cmd::new("mget").m("get").arity(-2).op(MGet).first(1).last(-1).step(1).padding(pt[6]).multi().key().bulk(),

        Cmd::new("set").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("incr").arity(2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("decr").arity(2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),

        // multi请求：异常响应需要改为$-1
        //("mincr","mincr",         -2, Store, 1, -1, 1, 3, true, false, true, false, true),
        // Cmd::new("mincr").arity(-2).op(Store).first(1).last(-1).step(1).padding(pt[3]).multi().key().bulk().nil_rsp(6),
        Cmd::new("mincr").arity(-2).op(Store).first(1).last(-1).step(1).padding(pt[6]).multi().key().bulk(),

        ////mset、del 是mlti指令，但只返回一个result，即need_bulk_num为false，那就只返回第一个key的响应 fishermen
        //// mset不需要bulk number
        //("mset", "set",           -3, Store, 1, -1, 2, 3, true, false, true, true, false),
        //// TODO: del 删除多个key时，返回删除的key数量，先不聚合这个数字，反正client也会忽略？ fishermen
        //("del", "del",            -2, Store, 1, -1, 1, 3, true, false, true, false, false),

        //// TODO：exists 虽然原生支持多key，但业务client支持单key，故此处只支持单key fishermen
        //("exists", "exists",       2, Get, 1, 1, 1, 3, false, false, true, false, false),

        //("expire",   "expire",     3, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("expireat", "expireat",   3, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("pexpire",  "pexpire",    3, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("pexpireat", "pexpireat", 3, Store, 1, 1, 1, 3, false, false, true, false, false),

        //("persist", "persist",     2, Store, 1, 1, 1, 3, false, false, true, false, false),
        Cmd::new("mset").m("set").arity(-3).op(Store).first(1).last(-1).step(2).padding(pt[3]).multi().key().val(),
        Cmd::new("del").arity(-2).op(Store).first(1).last(-1).step(1).padding(pt[3]).multi().key(),

        // 即便应对多语言，exists 也只支持一个key，否则需要计算多个后端数据，作为一个数字返回 fishermen
        Cmd::new("exists").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("expire").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("expireat").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("pexpire").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("pexpireat").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("persist").arity(2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),

        // zset 相关指令
        //("zadd", "zadd",                         -4, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("zincrby", "zincrby",                    4, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("zrem", "zrem",                         -3, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("zremrangebyrank", "zremrangebyrank",    4, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("zremrangebyscore", "zremrangebyscore",  4, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("zremrangebylex", "zremrangebylex",      4, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("zrevrange", "zrevrange",               -4, Get, 1, 1, 1, 3, false, false, true, false, false),

        //("zcard" , "zcard",                       2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("zrange", "zrange",                     -4, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("zrank", "zrank",                        3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("zrangebyscore", "zrangebyscore",       -4, Get, 1, 1, 1, 3, false, false, true, false, false),
        Cmd::new("zadd").arity(-4).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zincrby").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zrem").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zremrangebyrank").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zremrangebyscore").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zremrangebylex").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zrevrange").arity(-4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zcard").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zrange").arity(-4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zrank").arity(3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zrangebyscore").arity(-4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),

        // TODO: 验证先不支持这两个，避免在 hash冲突 vs 栈溢出 之间摇摆，或者后续把这个放到堆上？ fishermen
        //("zrevrank", "zrevrank",                  3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("zrevrangebyscore", "zrevrangebyscore", -4, Get, 1, 1, 1, 3, false, false, true, false, false),

        //("zrangebylex", "zrangebylex",           -4, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("zrevrangebylex", "zrevrangebylex",     -4, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("zcount", "zcount",                      4, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("zlexcount", "zlexcount",                4, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("zscore", "zscore",                      3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("zscan", "zscan",                       -3, Get, 1, 1, 1, 3, false, false, true, false, false),
        Cmd::new("zrevrank").arity(3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zrevrangebyscore").arity(-4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zrangebylex").arity(-4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zrevrangebylex").arity(-4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zcount").arity(4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zlexcount").arity(4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zscore").arity(3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("zscan").arity(-3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),

        // hash 相关 multi, noforward, has_key, has_val, need_bulk_num
        //("hset", "hset",                          -4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("hsetnx", "hsetnx",                      4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("hmset","hmset",                        -4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("hincrby", "hincrby",                    4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("hincrbyfloat", "hincrbyfloat",          4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("hdel", "hdel",                         -3, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("hget", "hget",                          3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("hgetall", "hgetall",                    2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("hlen", "hlen",                          2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("hkeys", "hkeys",                        2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("hmget", "hmget",                       -3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("hvals", "hvals",                        2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("hexists", "hexists",                    3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("hscan", "hscan",                        -3, Get, 1, 1, 1, 3, false, false, true, false, false),
        // hset 支持多field、value，hmset后续会被deprecated
        Cmd::new("hset").arity(-4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("hsetnx").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("hmset").arity(-4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("hincrby").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("hincrbyfloat").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("hdel").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("hget").arity(3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("hgetall").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("hlen").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("hkeys").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("hmget").arity(-3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("hvals").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("hexists").arity(3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("hscan").arity(-3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),

        // TODO 常规结构指令，测试完毕后，调整位置
        //("ttl", "ttl",                             2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("pttl", "pttl",                           2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("setnx", "setnx",                         3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("setex", "setex",                         4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("append", "append",                       3, Store, 1, 1, 1, 3, false, false, true, true, false),
        Cmd::new("ttl").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("pttl").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("setnx").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("setex").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("append").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),

        // longset 相关指令
        //("lsset", "lsset",                         4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("lsdset", "lsdset",                       4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("lsput", "lsput",                         -3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("lsdel", "lsdel",                         -3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("lsmexists", "lsmexists",                 -3, Get, 1, 1, 1, 3, false, false, true, true, false),
        //("lsgetall", "lsgetall",                   2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("lsdump", "lsdump",                       2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("lslen", "lslen",                         2, Get, 1, 1, 1, 3, false, false, true, false, false),
        // 根据eredis 3.1 修改
        Cmd::new("lsset").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("lsdset").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("lsput").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("lsdel").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("lsmexists").arity(-3).op(Get).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("lsgetall").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("lsdump").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("lslen").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),

        // list 相关指令
        //("rpush", "rpush",                         -3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("lpush", "lpush",                         -3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("rpushx", "rpushx",                       -3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("lpushx", "lpushx",                       -3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("linsert", "linsert",                      5, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("lset", "lset",                            4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("rpop", "rpop",                            2, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("lpop", "lpop",                            2, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("llen", "llen",                            2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("lindex", "lindex",                        3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("lrange", "lrange",                        4, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("ltrim", "ltrim",                          4, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("lrem", "lrem",                            4, Store, 1, 1, 1, 3, false, false, true, false, false),
        Cmd::new("rpush").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("lpush").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("rpushx").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("lpushx").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("linsert").arity(5).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("lset").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("rpop").arity(2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("lpop").arity(2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("llen").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("lindex").arity(3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("lrange").arity(4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("ltrim").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("lrem").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),

        // string 相关指令，包括 bit, str
        //("setbit", "setbit",                        4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("getbit", "getbit",                        3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("bitcount", "bitcount",                   -2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("bitpos", "bitpos",                       -3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("bitfield", "bitfield",                   -2, Store, 1, 1, 1, 3, false, false, true, false, false),

        //("setrange", "setrange",                    4, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("getrange", "getrange",                    4, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("getset", "getset",                        3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("strlen", "strlen",                        2, Get, 1, 1, 1, 3, false, false, true, false, false),
        Cmd::new("setbit").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("getbit").arity(3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("bitcount").arity(-2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("bitpos").arity(-3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("bitfield").arity(-2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("setrange").arity(4).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("getrange").arity(4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("getset").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("strlen").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),

        // 测试完毕后规整到incr附近
        //("incrby", "incrby",                        3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("decrby", "decrby",                        3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("incrbyfloat", "incrbyfloat",              3, Store, 1, 1, 1, 3, false, false, true, true, false),
        Cmd::new("incrby").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("decrby").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("incrbyfloat").arity(3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),

        // set 相关指令
        //("sadd", "sadd",                           -3, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("srem", "srem",                           -3, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("sismember", "sismember",                  3, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("scard", "scard",                          2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("spop", "spop",                           -2, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("srandmember", "srandmember",             -2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("smembers", "smembers",                    2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("sscan", "sscan",                         -3, Get, 1, 1, 1, 3, false, false, true, false, false),
        Cmd::new("sadd").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("srem").arity(-3).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("sismember").arity(3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("scard").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("spop").arity(-2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("srandmember").arity(-2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("smembers").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("sscan").arity(-3).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),

        // geo 相关指令
        //("geoadd", "geoadd",                       -5, Store, 1, 1, 1, 3, false, false, true, true, false),
        //("georadius", "georadius",                 -6, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("georadiusbymember", "georadiusbymember", -5, Store, 1, 1, 1, 3, false, false, true, false, false),
        //("geohash", "geohash",                     -2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("geopos", "geopos",                       -2, Get, 1, 1, 1, 3, false, false, true, false, false),
        //("geodist", "geodist",                     -4, Get, 1, 1, 1, 3, false, false, true, false, false),
        Cmd::new("geoadd").arity(-5).op(Store).first(1).last(1).step(1).padding(pt[3]).key().val(),
        Cmd::new("georadius").arity(-6).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("georadiusbymember").arity(-5).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("geohash").arity(-2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("geopos").arity(-2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("geodist").arity(-4).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),

        // pf相关指令
        //("pfadd", "pfadd",                         -2, Store, 1, 1, 1, 3, false, false, true, false, false),
        Cmd::new("pfadd").arity(-2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),

        // swallowed扩展指令，属性在add_support方法中增加 fishermen
        //("hashkeyq", "hashkeyq",                   2,  Meta,  1, 1, 1, 5, false, true, true, false, false),
        //("hashrandomq", "hashrandomq",             1,  Meta,  0, 0, 0, 5, false, true, false, false, false),
        Cmd::new("hashkeyq").arity(2).op(Meta).first(1).last(1).step(1).padding(pt[5]).
        nofwd().key().resv_hash().swallow().cmd_type(CommandType::SwallowedCmdHashkeyq),
        Cmd::new("hashrandomq").arity(1).op(Meta).padding(pt[5]).nofwd().resv_hash().swallow().
        cmd_type(CommandType::SwallowedCmdHashrandomq),
        
        Cmd::new("sendtoall").arity(1).op(Meta).padding(pt[5]).nofwd().cmd_type(CommandType::CmdSendToAll),
        Cmd::new("sendtoallq").arity(1).op(Meta).padding(pt[5]).nofwd().swallow().cmd_type(CommandType::CmdSendToAllq),

        // swallowed扩展指令对应的有返回值的指令，去掉q即可
        //("hashkey", "hashkey",                     2,  Get,  1, 1, 1, 5, false, true, true, false, false),
        //("keyshard", "keyshard",                  -2,  Get, 1, -1, 1, 5, true,  true, true, false, true),
        // 这个指令暂无需求，先不支持
        // ("hashrandom", "hashrandom",               1,  Meta,  0, 0, 0, 5, false, true, false, false, false),
        // hashkey、keyshard 改为meta，确保构建rsp时的status管理
        Cmd::new("hashkey").arity(2).op(Meta).first(1).last(1).step(1).padding(pt[5]).nofwd().key().resv_hash().
        cmd_type(CommandType::SpecLocalCmdHashkey),
        Cmd::new("keyshard").arity(-2).op(Meta).first(1).last(-1).step(1).padding(pt[5]).multi().
        nofwd().key().bulk().resv_hash().cmd_type(CommandType::SpecLocalCmdKeyshard),

        // lua script 相关指令，不解析相关key，由hashkey提前指定，业务一般在操作check+变更的事务时使用 fishermen\
        //("script", "script",                       -2, Store, 0, 0, 0, 3, false, false, false, false, false),
        //("evalsha", "evalsha",                     -3, Store, 0, 0, 0, 3, false, false, false, false, false),
        //("eval" , "eval",                          -3, Store, 0, 0, 0, 3, false, false, false, false, false),
        Cmd::new("script").arity(-2).op(Store).padding(pt[3]).need_resv_hash(),
        Cmd::new("evalsha").arity(-3).op(Store).padding(pt[3]).need_resv_hash(),
        Cmd::new("eval").arity(-3).op(Store).padding(pt[3]).need_resv_hash(),

        //phantom
        Cmd::new("bfget").arity(2).op(Get).first(1).last(1).step(1).padding(pt[3]).key(),
        Cmd::new("bfset").arity(2).op(Store).first(1).last(1).step(1).padding(pt[3]).key(),
        // bfmget、bfmset，padding改为5， fishermen
        Cmd::new("bfmget").m("bfget").arity(-2).op(MGet).first(1).last(-1).step(1).padding(pt[7]).multi().key().bulk(),
        Cmd::new("bfmset").m("bfset").arity(-2).op(Store).first(1).last(1).step(1).padding(pt[7]).multi().key().bulk(),
        // 待支持
        // {"lsmalloc",lsmallocCommand,3,REDIS_CMD_DENYOOM|REDIS_CMD_WRITE,NULL,1,1,1},
        // {"unlink",unlinkCommand,-2,REDIS_CMD_WRITE,NULL,1,-1,1},

        // {"riskauth",riskAuthCommand,2,0,NULL,0,0,0},

        // 管理类风险指令，暂不考虑支持
        // {"dbslots",dbslotsCommand,1,0,NULL,0,0,0},
        // {"save",saveCommand,1,0,NULL,0,0,0},
        // {"bgsave",bgsaveCommand,1,0,NULL,0,0,0},
        // {"shutdown",shutdownCommand,-1,0,NULL,0,0,0},
        // {"flushdb",flushdbCommand,1,REDIS_CMD_WRITE,NULL,0,0,0},
        // {"flushall",flushallCommand,1,REDIS_CMD_WRITE,NULL,0,0,0},
        // {"post",securityWarningCommand,-1,0,NULL,0,0,0},
        // {"host:",securityWarningCommand,-1,0,NULL,0,0,0},


        // 复制类指令，不支持
        // {"sync",syncCommand,1,0,NULL,0,0,0},
        // {"syncfrom",syncFromCommand,3,0,NULL,0,0,0},
        // {"replconf",replconfCommand,-1,0,NULL,0,0,0},
        // {"slaveof",slaveofCommand,3,0,NULL,0,0,0},
        // {"role",roleCommand,1,0,NULL,0,0,0},
        // {"rotate_aof",rotateAofCommand,1,0,NULL,0,0,0},

        // 普通管理类指令，暂不支持
        // {"monitor",monitorCommand,1,0,NULL,0,0,0},
        // {"lastsave",lastsaveCommand,1,0,NULL,0,0,0},
        // {"type",typeCommand,2,0,NULL,1,1,1},
         // {"tm",tmCommand,3,REDIS_CMD_WRITE,NULL,0,0,0},
        // {"version",versionCommand,1,0,NULL,0,0,0},
        // {"debug",debugCommand,-2,0,NULL,0,0,0},

        // 订阅类指令，暂不支持
        // {"unsubscribe",unsubscribeCommand,-1,0,NULL,0,0,0},
        // {"psubscribe",psubscribeCommand,-2,0,NULL,0,0,0},
        // {"punsubscribe",punsubscribeCommand,-1,0,NULL,0,0,0},
        // {"publish",publishCommand,3,REDIS_CMD_FORCE_REPLICATION|REDIS_CMD_WRITE,NULL,0,0,0},
        // "subscribe" => (-2, Operation::Get, 0, 0, 0),
        // {"pubsub", pubsubCommand, -2, REDIS_CMD_READONLY|REDIS_CMD_PUBSUB, NULL, 0, 0, 0},

        // 特殊指令，暂不支持
        // {"watch",watchCommand,-2,0,NULL,1,-1,1},
        // {"unwatch",unwatchCommand,1,0,NULL,0,0,0},
        // {"object",objectCommand,-2,0,NULL,2,2,1},

        // 涉及多个key，先不支持了
        // ("pfcount", "pfcount", -2, Get, 1, -1, 1, 3, false, false, true, false),
        // "pfmerge" => (-2, Operation::Store, 1, -1, 1),
        // ("pfselftest", "pfselftest", 1, Get, 1, 1, 1, 3, false, false, true, false),
        // "pfdebug" => (-3, Operation::Store, 0, 0, 0),


        // TODO: 暂时不支持的指令，启用时注意加上padding rsp fishermen
        // "psetex" => (4, Operation::Store, 1, 1, 1),
        // "substr" => (4, Operation::Get, 1, 1, 1),
        // "rpoplpush" => (3, Operation::Store, 1, 2, 1),
        // "brpop" => (-3, Operation::Store, 1, -2, 1),
        // "blpop" => (-3, Operation::Store, 1, -2, 1),
        // "brpoplpush" => (4, Operation::Store, 1, 2, 1),

        // 涉及多个key的操作，暂不支持
        // "bitop" => (-4, Operation::Store, 2, -1, 1),
        // "sinter" => (-2, Operation::Get, 1, -1, 1),
        // "sinterstore" => (-3, Operation::Store, 1, -1, 1),
        // "sunion" => (-2, Operation::Get, 1, -1, 1),
        // "sunionstore" => (-3, Operation::Store, 1, -1, 1),
        // "sdiff" => (-2, Operation::Get, 1, -1, 1),
        // "sdiffstore" => (-3, Operation::Store, 1, -1, 1),
        // "smove" => (4, Operation::Store, 1, 2, 1),
        // "zunionstore" => (-4, Operation::Store, 0, 0, 0),
        // "zinterstore" => (-4, Operation::Store, 0, 0, 0),

        // "hstrlen" => (3, Operation::Get, 1, 1, 1),

        // "msetnx" => (-3, Operation::Store, 1, -1, 2),
        // "randomkey" => (1, Operation::Get, 0, 0, 0),
        // "move" => (3, Operation::Store, 1, 1, 1),
        // "rename" => (3, Operation::Store, 1, 2, 1),
        // "renamenx" => (3, Operation::Store, 1, 2, 1),
        // "keys" => (2, Operation::Get, 0, 0, 0),
        // "scan" => (-2, Operation::Get, 0, 0, 0),
        // "dbsize" => (1, Operation::Get, 0, 0, 0),
        // "auth" => (2, Operation::Meta, 0, 0, 0),
        // "echo" => (2, Operation::Meta, 0, 0, 0),
        // info 先不在client支持
        // "info" => (-1, Operation::Meta, 0, 0, 0),

        // "config" => (-2, Operation::Meta, 0, 0, 0),

        // "time" => (1, Operation::Get, 0, 0, 0),

        // ********** 二期实现
        // 事务类、脚本类cmd，暂时先不支持，二期再处理 fishermen
        // "multi" => (1, Operation::Store, 0, 0, 0),
        // "exec" => (1, Operation::Store, 0, 0, 0),
        // "discard" => (1, Operation::Get, 0, 0, 0),
        // "sort" => (-2, Operation::Store, 1, 1, 1),
        // "client" => (-2, Operation::Meta, 0, 0, 0),

        // "slowlog" => (-2, Operation::Get, 0, 0, 0),
        // "wait" => (3, Operation::Meta, 0, 0, 0),
        // "latency" => (-2, Operation::Meta, 0, 0, 0),
    ] {
        cmds.add_support(c);
    }
    cmds
};

impl CommandProperties {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            mname: name,
            mname_len: name.len().to_string(),
            ..Default::default()
        }
    }
    pub(crate) fn m(mut self, mname: &'static str) -> Self {
        self.mname = mname;
        self.mname_len = mname.len().to_string();
        self
    }
    pub(crate) fn arity(mut self, arity: i8) -> Self {
        self.arity = arity;
        self
    }
    pub(crate) fn op(mut self, op: Operation) -> Self {
        self.op = op;
        self
    }
    pub(crate) fn first(mut self, first_key_index: u8) -> Self {
        self.first_key_index = first_key_index;
        self
    }
    pub(crate) fn last(mut self, last_key_idx: i8) -> Self {
        self.last_key_index = last_key_idx;
        self
    }
    pub(crate) fn step(mut self, key_step: u8) -> Self {
        self.key_step = key_step;
        self
    }
    pub(crate) fn padding(mut self, padding_rsp: &'static str) -> Self {
        self.padding_rsp = padding_rsp;
        self
    }
    pub(crate) fn key(mut self) -> Self {
        self.has_key = true;
        self
    }
    pub(crate) fn val(mut self) -> Self {
        self.has_val = true;
        self
    }
    pub(crate) fn nofwd(mut self) -> Self {
        self.noforward = true;
        self
    }
    pub(crate) fn multi(mut self) -> Self {
        self.multi = true;
        self
    }
    pub(crate) fn bulk(mut self) -> Self {
        self.need_bulk_num = true;
        self
    }
    pub(crate) fn resv_hash(mut self) -> Self {
        self.reserve_hash = true;
        self
    }
    pub(crate) fn need_resv_hash(mut self) -> Self {
        self.need_reserved_hash = true;
        self
    }
    pub(crate) fn swallow(mut self) -> Self {
        self.swallowed = true;
        self
    }

    // TODO 把nil 和 padding rsp整合，测试完毕后清理
    // fn nil_rsp(mut self, idx: u8) -> Self {
    //     assert!(idx < PADDING_RSP_TABLE.len() as u8);
    //     self.nil_rsp = idx;
    //     self
    // }
    pub(crate) fn master(mut self) -> Self {
        self.master_next = true;
        self
    }
    pub(crate) fn quit(mut self) -> Self {
        self.quit = true;
        self
    }
    pub(crate) fn cmd_type(mut self, cmd_type: CommandType) -> Self {
        self.cmd_type = cmd_type;
        self
    }
}
