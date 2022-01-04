use crate::{HashedCommand, OpCode, Operation};
use ds::{MemGuard, RingSlice};
use sharding::hash::{Crc32, Hash, UppercaseHashKey};

// 指令参数需要配合实际请求的token数进行调整，所以外部使用都通过方法获取
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
    pub(super) has_val: bool,
    pub(super) has_key: bool,
    pub(super) noforward: bool,
    pub(super) supported: bool,
    pub(super) multi: bool, // 该命令是否可能会包含多个key
    // need bulk number只对multi key请求的有意义
    pub(super) need_bulk_num: bool, // mset所有的请求只返回一个+OK，不需要在首个请求前加*bulk_num。其他的都需要
}

// 默认响应
// 第0个表示quit
pub const PADDING_RSP_TABLE: [&str; 4] =
    ["", "+OK\r\n", "+PONG\r\n", "-ERR redis no available\r\n"];

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
        debug_assert!(
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
    #[inline(always)]
    pub fn noforward(&self) -> bool {
        self.noforward
    }
    #[inline(always)]
    pub(super) fn flag(&self) -> crate::Flag {
        use super::flag::RedisFlager;
        let mut flag = crate::Flag::from_op(self.op_code, self.op);
        flag.set_padding_rsp(self.padding_rsp);
        if self.noforward {
            flag.set_noforward();
        }

        flag
    }
    // bulk_num只有在first=true时才有意义。
    #[inline]
    pub(super) fn build_request(
        &self,
        hash: i64,
        bulk_num: u16,
        first: bool,
        data: &RingSlice,
    ) -> HashedCommand {
        use ds::Buffer;
        debug_assert!(self.name.len() < 10);
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
        data.copy_to_vec(&mut cmd);
        let mut flag = self.flag();
        use super::flag::RedisFlager;
        if first {
            flag.set_mkey_first();
            // mset只有一个返回值。
            // 其他的multi请求的key的数量就是bulk_num
            debug_assert!(self.key_step == 1 || self.key_step == 2);
            let mut key_num = bulk_num;
            if self.key_step == 2 {
                key_num >>= 1;
            }
            flag.set_key_count(key_num);
        }
        let cmd: MemGuard = MemGuard::from_vec(cmd);
        HashedCommand::new(cmd, hash, flag)
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
    #[inline(always)]
    pub(crate) fn get_op_code(&self, name: &ds::RingSlice) -> u16 {
        let uppercase = UppercaseHashKey::new(name);
        let idx = self.hash.hash(&uppercase) as usize & (Self::MAPPING_RANGE - 1);
        // op_code 0表示未定义。不存在
        debug_assert_ne!(idx, 0);
        idx as u16
    }
    #[inline(always)]
    pub(crate) fn get_by_op(&self, op_code: u16) -> crate::Result<&CommandProperties> {
        debug_assert!((op_code as usize) < self.supported.len());
        let cmd = unsafe { self.supported.get_unchecked(op_code as usize) };
        if cmd.supported {
            Ok(cmd)
        } else {
            Err(crate::Error::CommandNotSupported)
        }
    }
    // 不支持会返回协议错误
    #[inline(always)]
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
        padding_rsp: u8,
        multi: bool,
        noforward: bool,
        has_key: bool,
        has_val: bool,
        need_bulk_num: bool,
    ) {
        let uppercase = name.to_uppercase();
        let idx = self.hash.hash(&uppercase.as_bytes()) as usize & (Self::MAPPING_RANGE - 1);
        debug_assert!(idx < self.supported.len());
        // 之前没有添加过。
        debug_assert!(!self.supported[idx].supported);
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
            noforward,
            supported: true,
            multi,
            has_key,
            has_val,
            need_bulk_num,
        };
    }
}

#[inline(always)]
pub(super) fn get_op_code(cmd: &ds::RingSlice) -> u16 {
    SUPPORTED.get_op_code(cmd)
}
#[inline(always)]
pub(super) fn get_cfg<'a>(op_code: u16) -> crate::Result<&'a CommandProperties> {
    SUPPORTED.get_by_op(op_code)
}
lazy_static! {
   pub(super) static ref SUPPORTED: Commands = {
        let mut cmds = Commands::new();
        use Operation::*;
    for (name, mname, arity, op, first_key_index, last_key_index, key_step, padding_rsp, multi, noforward, has_key, has_val, need_bulk_num)
        in vec![
                // meta 指令
                ("command", "command" ,-1, Meta, 0, 0, 0, 1, false, true, false, false, false),
                ("ping", "ping" ,-1, Meta, 0, 0, 0, 2, false, true, false, false, false),
                // 不支持select 0以外的请求。所有的select请求直接返回，默认使用db0
                ("select", "ping" ,2, Meta, 0, 0, 0, 1, false, true, false, false, false),
                ("quit", "quit" ,2, Meta, 0, 0, 0, 0, false, true, false, false, false),

                ("get" , "get",2, Get, 1, 1, 1, 3, false, false, true, false, false),
                ("mget", "get", -2, MGet, 1, -1, 1, 3, true, false, true, false, true),

                ("set" ,"set", 3, Store, 1, 1, 1, 3, false, false, true, true, false),
                ("incr" ,"incr", 2, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("decr" ,"decr", 2, Store, 1, 1, 1, 3, false, false, true, false, false),
                ("mincr","incr",  -2, Store, 1, -1, 1, 3, true, false, true, false, true),
                // mset不需要bulk number
                ("mset", "set", -3, Store, 1, -1, 2, 3, true, false, true, true, false),

            // TODO: 随着测试，逐步打开，注意加上padding rsp fishermen
            // "setnx" => (3, Operation::Store, 1, 1, 1),
            // "setex" => (4, Operation::Store, 1, 1, 1),
            // "psetex" => (4, Operation::Store, 1, 1, 1),
            // "append" => (3, Operation::Store, 1, 1, 1),
            // "strlen" => (2, Operation::Get, 1, 1, 1),
            // "del" => (-2, Operation::Store, 1, -1, 1),
            // "exists" => (-2, Operation::Get, 1, -1, 1),
            // "setbit" => (4, Operation::Store, 1, 1, 1),
            // "getbit" => (3, Operation::Get, 1, 1, 1),
            // "setrange" => (4, Operation::Store, 1, 1, 1),
            // "getrange" => (4, Operation::Get, 1, 1, 1),
            // "substr" => (4, Operation::Get, 1, 1, 1),
            // "incr" => (2, Operation::Store, 1, 1, 1),
            // "decr" => (2, Operation::Store, 1, 1, 1),
            // "mget" => (-2, Operation::MGet, 1, -1, 1),
            // "rpush" => (-3, Operation::Store, 1, 1, 1),
            // "lpush" => (-3, Operation::Store, 1, 1, 1),
            // "rpushx" => (-3, Operation::Store, 1, 1, 1),
            // "lpushx" => (-3, Operation::Store, 1, 1, 1),
            // "linsert" => (5, Operation::Store, 1, 1, 1),
            // "rpop" => (2, Operation::Store, 1, 1, 1),
            // "lpop" => (2, Operation::Store, 1, 1, 1),
            // "rpoplpush" => (3, Operation::Store, 1, 2, 1),
            // "brpop" => (-3, Operation::Store, 1, -2, 1),
            // "blpop" => (-3, Operation::Store, 1, -2, 1),
            // "brpoplpush" => (4, Operation::Store, 1, 2, 1),
            // "llen" => (2, Operation::Get, 1, 1, 1),
            // "lindex" => (3, Operation::Get, 1, 1, 1),
            // "lset" => (4, Operation::Store, 1, 1, 1),
            // "lrange" => (4, Operation::Get, 1, 1, 1),
            // "ltrim" => (4, Operation::Get, 1, 1, 1),
            // "lrem" => (4, Operation::Get, 1, 1, 1),
            // "sadd" => (-3, Operation::Store, 1, 1, 1),
            // "srem" => (-3, Operation::Store, 1, 1, 1),
            // "smove" => (4, Operation::Store, 1, 2, 1),
            // "sismember" => (3, Operation::Get, 1, 1, 1),
            // "scard" => (2, Operation::Get, 1, 1, 1),
            // // 虽然是read类型指令，但涉及到删除集合中的元素，先当作store指令
            // "spop" => (-2, Operation::Store, 1, 1, 1),
            // "srandmember" => (-2, Operation::Get, 1, 1, 1),
            // "sinter" => (-2, Operation::Get, 1, -1, 1),
            // "sinterstore" => (-3, Operation::Store, 1, -1, 1),
            // "sunion" => (-2, Operation::Get, 1, -1, 1),
            // "sunionstore" => (-3, Operation::Store, 1, -1, 1),
            // "sdiff" => (-2, Operation::Get, 1, -1, 1),
            // "sdiffstore" => (-3, Operation::Store, 1, -1, 1),
            // "smembers" => (2, Operation::Get, 1, 1, 1),
            // "sscan" => (-3, Operation::Get, 1, 1, 1),
            // "zadd" => (-4, Operation::Store, 1, 1, 1),
            // "zincrby" => (4, Operation::Store, 1, 1, 1),
            // "zrem" => (-3, Operation::Store, 1, 1, 1),
            // "zremrangebyscore" => (4, Operation::Store, 1, 1, 1),
            // "zremrangebyrank" => (4, Operation::Store, 1, 1, 1),
            // "zremrangebylex" => (4, Operation::Store, 1, 1, 1),
            // "zunionstore" => (-4, Operation::Store, 0, 0, 0),
            // "zinterstore" => (-4, Operation::Store, 0, 0, 0),
            // "zrange" => (-4, Operation::Get, 1, 1, 1),
            // "zrevrange" => (-4, Operation::Get, 1, 1, 1),
            // "zrangebyscore" => (-4, Operation::Get, 1, 1, 1),
            // "zrevrangebyscore" => (-4, Operation::Get, 1, 1, 1),
            // "zrangebylex" => (-4, Operation::Get, 1, 1, 1),
            // "zrevrangebylex" => (-4, Operation::Get, 1, 1, 1),
            // "zcount" => (4, Operation::Get, 1, 1, 1),
            // "zlexcount" => (4, Operation::Get, 1, 1, 1),
            // "zcard" => (2, Operation::Get, 1, 1, 1),
            // "zscore" => (3, Operation::Get, 1, 1, 1),
            // "zrank" => (3, Operation::Get, 1, 1, 1),
            // "zrevrank" => (3, Operation::Get, 1, 1, 1),
            // "zscan" => (-3, Operation::Get, 1, 1, 1),
            // "hset" => (4, Operation::Store, 1, 1, 1),
            // "hsetnx" => (4, Operation::Store, 1, 1, 1),
            // "hget" => (3, Operation::Get, 1, 1, 1),
            // "hmset" => (-4, Operation::Store, 1, 1, 1),
            // "hmget" => (-3, Operation::Get, 1, 1, 1),
            // "hincrby" => (4, Operation::Store, 1, 1, 1),
            // "hincrbyfloat" => (4, Operation::Store, 1, 1, 1),
            // "hdel" => (-3, Operation::Store, 1, 1, 1),
            // "hlen" => (2, Operation::Get, 1, 1, 1),
            // "hstrlen" => (3, Operation::Get, 1, 1, 1),
            // "hkeys" => (2, Operation::Get, 1, 1, 1),
            // "hvals" => (2, Operation::Get, 1, 1, 1),
            // "hgetall" => (2, Operation::Get, 1, 1, 1),
            // "hexists" => (3, Operation::Get, 1, 1, 1),
            // "hscan" => (-3, Operation::Get, 1, 1, 1),
            // "incrby" => (3, Operation::Store, 1, 1, 1),
            // "decrby" => (3, Operation::Store, 1, 1, 1),
            // "incrbyfloat" => (3, Operation::Store, 1, 1, 1),
            // "getset" => (3, Operation::Store, 1, 1, 1),
            // "mset" => (-3, Operation::Store, 1, -1, 2),
            // "msetnx" => (-3, Operation::Store, 1, -1, 2),
            // "randomkey" => (1, Operation::Get, 0, 0, 0),
            // // "select" => (2, Operation::Meta, 0, 0, 0),
            // "move" => (3, Operation::Store, 1, 1, 1),
            // "rename" => (3, Operation::Store, 1, 2, 1),
            // "renamenx" => (3, Operation::Store, 1, 2, 1),
            // "expire" => (3, Operation::Store, 1, 1, 1),
            // "expireat" => (3, Operation::Store, 1, 1, 1),
            // "pexpire" => (3, Operation::Store, 1, 1, 1),
            // "pexpireat" => (3, Operation::Store, 1, 1, 1),
            // "keys" => (2, Operation::Get, 0, 0, 0),
            // "scan" => (-2, Operation::Get, 0, 0, 0),
            // "dbsize" => (1, Operation::Get, 0, 0, 0),
            // "auth" => (2, Operation::Meta, 0, 0, 0),
            // // "ping" => (-1, Operation::Meta, 0, 0, 0),
            // "echo" => (2, Operation::Meta, 0, 0, 0),
            // "info" => (-1, Operation::Meta, 0, 0, 0),
            // "ttl" => (2, Operation::Get, 1, 1, 1),
            // "pttl" => (2, Operation::Get, 1, 1, 1),
            // "persist" => (2, Operation::Store, 1, 1, 1),
            // "config" => (-2, Operation::Meta, 0, 0, 0),
            // "subscribe" => (-2, Operation::Get, 0, 0, 0),
            // "unsubscribe" => (-1, Operation::Get, 0, 0, 0),
            // "psubscribe" => (-2, Operation::Get, 0, 0, 0),
            // "punsubscribe" => (-1, Operation::Get, 0, 0, 0),
            // "publish" => (-1, Operation::Store, 0, 0, 0),
            // "pubsub" => (-1, Operation::Get, 0, 0, 0),
            // "watch" => (-2, Operation::Get, 1, -1, 1),
            // "unwatch" => (1, Operation::Get, 0, 0, 0),
            // "restore" => (-4, Operation::Store, 1, 1, 1),
            // "restore-asking" => (-4, Operation::Store, 1, 1, 1),
            // "migrate" => (-6, Operation::Store, 0, 0, 0),
            // "dump" => (2, Operation::Get, 1, 1, 1),
            // "object" => (3, Operation::Get, 2, 2, 2),
            // "evalsha" => (-3, Operation::Store, 0, 0, 0),
            // "script" => (-2, Operation::Get, 0, 0, 0),
            // "time" => (1, Operation::Get, 0, 0, 0),
            // "bitop" => (-4, Operation::Store, 2, -1, 1),
            // "bitcount" => (-2, Operation::Get, 1, 1, 1),
            // "bitpos" => (-3, Operation::Get, 1, 1, 1),
            // "geoadd" => (-5, Operation::Store, 1, 1, 1),
            // "georadius" => (-6, Operation::Get, 1, 1, 1),
            // "georadiusbymember" => (-5, Operation::Get, 1, 1, 1),
            // "geohash" => (-2, Operation::Get, 1, 1, 1),
            // "geopos" => (-2, Operation::Get, 1, 1, 1),
            // "geodist" => (-4, Operation::Get, 1, 1, 1),
            // "pfselftest" => (1, Operation::Get, 1, 1, 1),
            // "pfadd" => (-2, Operation::Store, 1, 1, 1),
            // "pfcount" => (-2, Operation::Get, 1, -1, 1),
            // "pfmerge" => (-2, Operation::Store, 1, -1, 1),
            // "pfdebug" => (-3, Operation::Store, 0, 0, 0),

            // ********** 二期实现
            // 事务类、脚本类cmd，暂时先不支持，二期再处理 fishermen
            // "multi" => (1, Operation::Store, 0, 0, 0),
            // "exec" => (1, Operation::Store, 0, 0, 0),
            // "discard" => (1, Operation::Get, 0, 0, 0),
            // "sort" => (-2, Operation::Store, 1, 1, 1),
            // "client" => (-2, Operation::Meta, 0, 0, 0),
            // "eval" => (-3, Operation::Store, 0, 0, 0),
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
        need_bulk_num
    ) ;
            }
        cmds
    };
}
