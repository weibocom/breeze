use crate::{HashedCommand, OpCode, Operation};
use ds::{MemGuard, RingSlice};
use sharding::hash::{Bkdr, Hash, HashKey, UppercaseHashKey};

pub const SWALLOWED_CMD_HASHKEYQ: &str = "hashkeyq";
pub const SWALLOWED_CMD_HASHRANDOMQ: &str = "hashrandomq";

// 指示下一个cmd
pub const DIST_CMD_HASHKEY: &str = "hashkey";
pub const DIST_CMD_KEYSHARD: &str = "keyshard";

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
    pub(super) padding_rsp: u8,
    pub(super) nil_rsp: u8,
    pub(super) has_val: bool,
    pub(super) has_key: bool,
    pub(super) noforward: bool,
    pub(super) supported: bool,
    pub(super) multi: bool, // 该命令是否可能会包含多个key
    // need bulk number只对multi key请求的有意义
    pub(super) need_bulk_num: bool, // mset所有的请求只返回一个+OK，不需要在首个请求前加*bulk_num。其他的都需要
    pub(super) swallowed: bool, // 该指令是否需要mesh 吞噬，吞噬后不会响应client、也不会发给后端server，吞噬指令一般用于指示下一个常规指令的额外处理属性
    pub(super) reserve_hash: bool, // 是否为下一个cmd指定hash，如果为true，将当前hash存储下来，供下一个cmd使用
    pub(super) need_reserved_hash: bool, // 是否需要前一个指令明确指定的hash，如果为true，则必须有key或者通过hashkey指定明确的hash
}

// 默认响应
// 第0个表示quit
pub const PADDING_RSP_TABLE: [&str; 7] = [
    "",
    "+OK\r\n",
    "+PONG\r\n",
    "-ERR redis no available\r\n",
    "-ERR invalid command\r\n",
    "-ERR should swallowed in mesh\r\n", // 仅仅占位，会在mesh内吞噬掉，不会返回给client or server
    "$-1\r\n",                           // mget 等指令对应的nil
];

#[allow(dead_code)]
impl CommandProperties {
    #[inline]
    pub fn operation(&self) -> &Operation {
        &self.op
    }

    #[inline]
    pub fn validate(&self, token_count: usize) -> bool {
        if self.arity == 0 {
            return false;
        }
        if self.arity > 0 {
            return token_count == self.arity as usize;
        } else {
            let last_key_idx = self.last_key_index(token_count);
            return token_count > last_key_idx && last_key_idx >= self.first_key_index();
        }
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

    pub fn padding_rsp(&self) -> u8 {
        self.padding_rsp
    }
    #[inline]
    pub fn noforward(&self) -> bool {
        self.noforward
    }

    pub(super) fn flag(&self) -> crate::Flag {
        let mut flag = crate::Flag::from_op(self.op_code, self.op);
        use super::flag::RedisFlager;
        flag.set_padding_rsp(self.padding_rsp);
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
        data: &RingSlice,
    ) -> HashedCommand {
        use ds::Buffer;
        assert!(self.name.len() < 10, "name:{}", self.name);
        let mut cmd = Vec::with_capacity(16 + data.len());
        cmd.push(b'*');
        // 1个cmd, 1个key，1个value。一共3个bulk
        cmd.push((2 + self.has_val as u8) + b'0');
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(self.mname.len().to_string());
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
        let cmd: MemGuard = MemGuard::from_vec(cmd);
        HashedCommand::new(cmd, hash, flag)
    }
}

// https://redis.io/commands 一共145大类命令。使用 crate::sharding::Hash::Crc32
// 算法能够完整的将其映射到0~4095这个区间。因为使用这个避免大量的match消耗。
pub(super) struct Commands {
    supported: [CommandProperties; Self::MAPPING_RANGE],
    // hash: Crc32,
    hash: Bkdr,
}
impl Commands {
    const MAPPING_RANGE: usize = 2048;
    fn new() -> Self {
        Self {
            supported: [CommandProperties::default(); Self::MAPPING_RANGE],
            // hash: Crc32::default(),
            hash: Bkdr::default(),
        }
    }
    #[inline]
    pub(crate) fn get_op_code(&self, name: &ds::RingSlice) -> u16 {
        let uppercase = UppercaseHashKey::new(name);
        // let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
        let idx = self.inner_hash(&uppercase);
        // op_code 0表示未定义,不存在
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
        // let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
        let idx = self.inner_hash(&uppercase);
        self.get_by_op(idx as u16)
    }

    #[inline]
    fn inner_hash<K: HashKey>(&self, key: &K) -> usize {
        let idx = self.hash.hash(key) as usize & (Self::MAPPING_RANGE - 1);
        // 由于op_code 0表示未定义,不存在,故对于0需要进行转换为1
        if idx == 0 {
            return 1;
        }
        idx
    }

    #[inline]
    fn add_support(
        &mut self,
        name: &'static str,
        mname: &'static str,
        arity: i8,
        op: Operation,
        first_key_index: u8,
        last_key_index: i8,
        key_step: u8,
        padding_rsp: u8,
        multi: bool,
        noforward: bool,
        has_key: bool,
        has_val: bool,
        need_bulk_num: bool,
    ) {
        let uppercase = name.to_uppercase();
        // let idx = self.hash.hash(&uppercase.as_bytes()) as usize & (Self::MAPPING_RANGE - 1);
        let idx = self.inner_hash(&uppercase.as_bytes());
        assert!(idx < self.supported.len(), "idx:{}", idx);
        // 之前没有添加过。
        assert!(!self.supported[idx].supported);

        /*===============  特殊属性，关联cmds极少，直接在这里设置  ===============*/
        // 吞噬cmd目前只有hashkeyq、hashrandomq
        let swallowed = uppercase.eq(&SWALLOWED_CMD_HASHKEYQ.to_uppercase())
            || uppercase.eq(&SWALLOWED_CMD_HASHRANDOMQ.to_uppercase());

        // 是否需要为下一个cmd 保留（指定）hash
        let reserve_hash = uppercase.eq(&DIST_CMD_HASHKEY.to_uppercase())
            || uppercase.eq(&DIST_CMD_KEYSHARD.to_uppercase())
            || uppercase.eq(&SWALLOWED_CMD_HASHKEYQ.to_uppercase())
            || uppercase.eq(&SWALLOWED_CMD_HASHRANDOMQ.to_uppercase());

        // 需要前一个指令明确指定hash的目前只有lua下面3个指令
        let need_reserved_hash =
            uppercase.eq("EVAL") || uppercase.eq("EVALSHA") || uppercase.eq("SCRIPT");
        // 目前只有mget指令是MGet/MINCR类型，才需要nil
        let mut nil_rsp = 0;
        if uppercase.eq("MGET") || uppercase.eq("MINCR") {
            nil_rsp = 6;
        }

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
            nil_rsp,
            noforward,
            supported: true,
            multi,
            has_key,
            has_val,
            need_bulk_num,
            swallowed,
            reserve_hash,
            need_reserved_hash,
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
        // TODO：后续增加新指令时，当multi/need_bulk_num 均为true时，需要在add_support中进行nil转换，避免将err返回到client fishermen
    for (name, mname, arity, op, first_key_index, last_key_index, key_step, padding_rsp, multi, noforward, has_key, has_val, need_bulk_num)
        in vec![
                // meta 指令
                ("command", "command" ,   -1, Meta, 0, 0, 0, 1, false, true, false, false, false),
                ("ping", "ping" ,         -1, Meta, 0, 0, 0, 2, false, true, false, false, false),
                // 不支持select 0以外的请求。所有的select请求直接返回，默认使用db0
                ("select", "select" ,      2, Meta, 0, 0, 0, 1, false, true, false, false, false),
                ("hello", "hello" ,        2, Meta, 0, 0, 0, 4, false, true, false, false, false),
                ("quit", "quit" ,          2, Meta, 0, 0, 0, 0, false, true, false, false, false),

                ("get" , "get",            2, Get, 1, 1, 1, 3, false, false, true, false, false),

                // multi请求：异常响应需要改为$-1
                ("mget", "get",           -2, MGet, 1, -1, 1, 3, true, false, true, false, true),

                ("set" ,"set",             3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("incr" ,"incr",           2, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("decr" ,"decr",           2, Store, 1, 1, 1, 3, false, false, true, false, false),

                // multi请求：异常响应需要改为$-1
                ("mincr","mincr",         -2, Store, 1, -1, 1, 3, true, false, true, false, true),

                //mset、del 是mlti指令，但只返回一个result，即need_bulk_num为false，那就只返回第一个key的响应 fishermen
                // mset不需要bulk number
                ("mset", "set",           -3, Store, 1, -1, 2, 3, true, false, true, true, false),
                // TODO: del 删除多个key时，返回删除的key数量，先不聚合这个数字，反正client也会忽略？ fishermen
                ("del", "del",            -2, Store, 1, -1, 1, 3, true, false, true, false, false),

                // TODO：exists 虽然原生支持多key，但业务client支持单key，故此处只支持单key fishermen
                ("exists", "exists",       2, Get, 1, 1, 1, 3, false, false, true, false, false),

                ("expire",   "expire",     3, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("expireat", "expireat",   3, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("pexpire",  "pexpire",    3, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("pexpireat", "pexpireat", 3, Store, 1, 1, 1, 3, false, false, true, false, false),

                ("persist", "persist",     2, Store, 1, 1, 1, 3, false, false, true, false, false),

                // zset 相关指令
                ("zadd", "zadd",                         -4, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("zincrby", "zincrby",                    4, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("zrem", "zrem",                         -3, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("zremrangebyrank", "zremrangebyrank",    4, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("zremrangebyscore", "zremrangebyscore",  4, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("zremrangebylex", "zremrangebylex",      4, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("zrevrange", "zrevrange",               -4, Get, 1, 1, 1, 3, false, false, true, false, false),

                ("zcard" , "zcard",                       2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("zrange", "zrange",                     -4, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("zrank", "zrank",                        3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("zrangebyscore", "zrangebyscore",       -4, Get, 1, 1, 1, 3, false, false, true, false, false),

                // TODO: 验证先不支持这两个，避免在 hash冲突 vs 栈溢出 之间摇摆，或者后续把这个放到堆上？ fishermen
                ("zrevrank", "zrevrank",                  3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("zrevrangebyscore", "zrevrangebyscore", -4, Get, 1, 1, 1, 3, false, false, true, false, false),

                ("zrangebylex", "zrangebylex",           -4, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("zrevrangebylex", "zrevrangebylex",     -4, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("zcount", "zcount",                      4, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("zlexcount", "zlexcount",                4, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("zscore", "zscore",                      3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("zscan", "zscan",                       -3, Get, 1, 1, 1, 3, false, false, true, false, false),

                // hash 相关 multi, noforward, has_key, has_val, need_bulk_num
                ("hset", "hset",                          4, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("hsetnx", "hsetnx",                      4, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("hmset","hmset",                        -4, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("hincrby", "hincrby",                    4, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("hincrbyfloat", "hincrbyfloat",          4, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("hdel", "hdel",                         -3, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("hget", "hget",                          3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("hgetall", "hgetall",                    2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("hlen", "hlen",                          2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("hkeys", "hkeys",                        2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("hmget", "hmget",                       -3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("hvals", "hvals",                        2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("hexists", "hexists",                    3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("hscan", "hscan",                        -3, Get, 1, 1, 1, 3, false, false, true, false, false),

                // TODO 常规结构指令，测试完毕后，调整位置
                ("ttl", "ttl",                             2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("pttl", "pttl",                           2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("setnx", "setnx",                         3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("setex", "setex",                         4, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("append", "append",                       3, Store, 1, 1, 1, 3, false, false, true, true, false),

                // longset 相关指令
                ("lsset", "lsset",                         -3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("lsdset", "lsdset",                       -3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("lsput", "lsput",                         -3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("lsdel", "lsdel",                         -3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("lsmexists", "lsmexists",                 -3, Get, 1, 1, 1, 3, false, false, true, true, false),
                ("lsgetall", "lsgetall",                   -3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("lsdump", "lsdump",                       -3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("lslen", "lslen",                         -3, Get, 1, 1, 1, 3, false, false, true, false, false),

                // list 相关指令
                ("rpush", "rpush",                         -3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("lpush", "lpush",                         -3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("rpushx", "rpushx",                       -3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("lpushx", "lpushx",                       -3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("linsert", "linsert",                      5, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("lset", "lset",                            4, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("rpop", "rpop",                            2, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("lpop", "lpop",                            2, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("llen", "llen",                            2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("lindex", "lindex",                        3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("lrange", "lrange",                        4, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("ltrim", "ltrim",                          4, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("lrem", "lrem",                            4, Store, 1, 1, 1, 3, false, false, true, false, false),

                // string 相关指令，包括 bit, str
                ("setbit", "setbit",                        4, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("getbit", "getbit",                        3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("bitcount", "bitcount",                   -2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("bitpos", "bitpos",                       -3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("bitfield", "bitfield",                   -2, Store, 1, 1, 1, 3, false, false, true, false, false),

                ("setrange", "setrange",                    4, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("getrange", "getrange",                    4, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("getset", "getset",                        3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("strlen", "strlen",                        2, Get, 1, 1, 1, 3, false, false, true, false, false),

                // 测试完毕后规整到incr附近
                ("incrby", "incrby",                        3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("decrby", "decrby",                        3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("incrbyfloat", "incrbyfloat",              3, Store, 1, 1, 1, 3, false, false, true, true, false),

                // set 相关指令
                ("sadd", "sadd",                           -3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("srem", "srem",                           -3, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("sismember", "sismember",                  3, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("scard", "scard",                          2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("spop", "spop",                           -2, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("srandmember", "srandmember",             -2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("smembers", "smembers",                    2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("sscan", "sscan",                         -3, Get, 1, 1, 1, 3, false, false, true, false, false),

                // geo 相关指令
                ("geoadd", "geoadd",                       -5, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("georadius", "georadius",                 -6, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("georadiusbymember", "georadiusbymember", -5, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("geohash", "geohash",                     -2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("geopos", "geopos",                       -2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("geodist", "geodist",                     -4, Get, 1, 1, 1, 3, false, false, true, false, false),

                // pf相关指令
                ("pfadd", "pfadd",                         -2, Store, 1, 1, 1, 3, false, false, true, false, false),

                // swallowed扩展指令，属性在add_support方法中增加 fishermen
                ("hashkeyq", "hashkeyq",                   2,  Meta,  1, 1, 1, 5, false, true, true, false, false),
                ("hashrandomq", "hashrandomq",             1,  Meta,  0, 0, 0, 5, false, true, false, false, false),

                // swallowed扩展指令对应的有返回值的指令，去掉q即可
                ("hashkey", "hashkey",                     2,  Get,  1, 1, 1, 5, false, true, true, false, false),
                ("keyshard", "keyshard",                  -2,  Get, 1, -1, 1, 5, true,  true, true, false, true),
                // 这个指令暂无需求，先不支持
                // ("hashrandom", "hashrandom",               1,  Meta,  0, 0, 0, 5, false, true, false, false, false),

                // lua script 相关指令，不解析相关key，由hashkey提前指定，业务一般在操作check+变更的事务时使用 fishermen\
                ("script", "script",                       -2, Store, 0, 0, 0, 3, false, false, false, false, false),
                ("evalsha", "evalsha",                     -3, Store, 0, 0, 0, 3, false, false, false, false, false),
                ("eval" , "eval",                          -3, Store, 0, 0, 0, 3, false, false, false, false, false),

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
        need_bulk_num,
    ) ;
            }
        cmds
    };
}
