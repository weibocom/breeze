use super::{
    command::{CommandHasher, CommandProperties, CommandType},
    error::RedisError,
};
use crate::{error::Error, redis::command, Flag, Result, StreamContext};
use ds::RingSlice;
use sharding::hash::Hash;

const CRLF_LEN: usize = b"\r\n".len();
// 这个context是用于中multi请求中，同一个multi请求中跨request协调
// 必须是u64长度的。
#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct RequestContext {
    pub bulk: u16,
    pub op_code: u16,
    pub first: bool,      // 在multi-get请求中是否是第一个请求。
    pub layer: u8,        // 请求的层次，目前只支持：master，all
    pub sendto_all: bool, //发送到所有shard
    //手动option，屏蔽需要对option repr的了解
    pub is_reserved_hash: bool,
    //16
    pub reserved_hash: i64,
}

impl From<&mut StreamContext> for RequestContext {
    fn from(value: &mut StreamContext) -> Self {
        unsafe { std::mem::transmute(*value) }
    }
}

impl From<RequestContext> for StreamContext {
    fn from(value: RequestContext) -> Self {
        unsafe { std::mem::transmute(value) }
    }
}

// 请求的layer层次，目前只有masterOnly，后续支持业务访问某层时，在此扩展属性
#[repr(u8)]
pub enum LayerType {
    MasterOnly = 1,
}

// impl RequestContext {
//     #[inline]
//     fn reset(&mut self) {
//         assert_eq!(std::mem::size_of::<Self>(), 8);
//         *self.u64_mut() = 0;
//     }
//     #[inline]
//     fn u64(&mut self) -> u64 {
//         *self.u64_mut()
//     }
//     #[inline]
//     fn u64_mut(&mut self) -> &mut u64 {
//         unsafe { std::mem::transmute(self) }
//     }
//     #[inline]
//     fn from(v: u64) -> Self {
//         unsafe { std::mem::transmute(v) }
//     }
// }

//包含流解析过程中当前命令和前面命令的状态
pub(crate) struct RequestPacket<'a, S> {
    stream: &'a mut S,
    data: Packet,
    // 低16位是bulk_num
    // 次低16位是op_code.
    ctx: RequestContext,
    oft_last: usize,
    oft: usize,
}

impl<'a, S: crate::Stream> RequestPacket<'a, S> {
    #[inline]
    pub(crate) fn new(stream: &'a mut S) -> Self {
        let ctx = stream.context().into();
        let data = stream.slice();
        Self {
            oft_last: 0,
            oft: 0,
            data: Packet { inner: data },
            ctx,
            stream,
        }
    }

    #[inline]
    pub(crate) fn has_bulk(&self) -> bool {
        self.bulk() > 0
    }
    #[inline]
    pub(crate) fn available(&self) -> bool {
        self.oft < self.data.len()
    }

    #[inline]
    pub(crate) fn parse_bulk_num(&mut self) -> Result<()> {
        if self.bulk() == 0 {
            debug_assert!(self.available(), "{:?}", self);
            if self.data[self.oft] != b'*' {
                return Err(RedisError::ReqInvalidStar.into());
            }
            self.ctx.bulk = self.data.num_of_bulks(&mut self.oft)? as u16;
            self.ctx.first = true;
            assert_ne!(self.bulk(), 0, "packet:{}", self);
        }
        Ok(())
    }
    //#[inline(always)]
    //fn incr_oft(&mut self, by: usize) -> Result<()> {
    //    self.oft += by;
    //    if self.oft > self.data.len() {
    //        return Err(crate::Error::ProtocolIncomplete);
    //    }
    //    Ok(())
    //}
    #[inline]
    pub(crate) fn parse_cmd(&mut self) -> Result<&'static CommandProperties> {
        // 需要确保，如果op_code不为0，则之前的数据一定处理过。
        if self.ctx.op_code == 0 {
            // 当前上下文是获取命令。格式为:  $num\r\ncmd\r\n
            if let Some(first_r) = self.data.find(self.oft, b'\r') {
                debug_assert_eq!(self.data[self.oft], b'$', "{:?}", self);
                // 路过CRLF_LEN个字节，通过命令获取op_code
                let (op_code, idx) = CommandHasher::hash_slice(&*self.data, first_r + CRLF_LEN)?;
                assert!(idx + CRLF_LEN <= self.data.len());
                self.ctx.op_code = op_code;
                // 第一次解析cmd需要对协议进行合法性校验
                let cfg = command::get_cfg(op_code)?;
                cfg.validate(self.bulk() as usize)?;

                if cfg.need_reserved_hash && !(self.sendto_all() || self.ctx.is_reserved_hash) {
                    return Err(RedisError::ReqInvalid.into());
                }
                // check 命令长度
                debug_assert_eq!(cfg.name.len(), self.data.str_num(self.oft + 1..first_r));

                // cmd name 解析完毕，bulk 减 1
                self.oft = idx + CRLF_LEN;
                self.ctx.bulk -= 1;
                Ok(cfg)
            } else {
                return Err(crate::Error::ProtocolIncomplete);
            }
        } else {
            command::get_cfg(self.op_code())
        }
    }
    #[inline]
    pub(crate) fn parse_key(&mut self) -> Result<RingSlice> {
        debug_assert_ne!(self.ctx.op_code, 0, "packet:{:?}", self);
        debug_assert_ne!(self.ctx.bulk, 0, "packet:{:?}", self);
        let key_len = self.data.num_and_skip(&mut self.oft)?;
        self.ctx.bulk -= 1;
        let start = self.oft - CRLF_LEN - key_len;
        Ok(self.data.sub_slice(start, key_len))
    }
    #[inline]
    pub(crate) fn ignore_one_bulk(&mut self) -> Result<()> {
        assert_ne!(self.ctx.bulk, 0, "packet:{:?}", self);
        self.data.num_and_skip(&mut self.oft)?;
        self.ctx.bulk -= 1;
        Ok(())
    }
    #[inline]
    pub(crate) fn ignore_all_bulks(&mut self) -> Result<()> {
        while self.bulk() > 0 {
            self.ignore_one_bulk()?;
        }
        Ok(())
    }
    // 忽略掉之前的数据，通常是multi请求的前面部分。
    #[inline]
    pub(crate) fn multi_ready(&mut self) {
        if self.oft > self.oft_last {
            self.stream.ignore(self.oft - self.oft_last);
            self.oft_last = self.oft;

            assert_ne!(self.ctx.op_code, 0, "packet:{}", self);
            // 更新
            *self.stream.context() = self.ctx.into();
        }
    }

    // 重置context，包括stream中的context
    #[inline]
    fn reset_context(&mut self) {
        // 重置packet的ctx
        self.ctx = Default::default();

        // 重置stream的ctx
        *self.stream.context() = Default::default();
    }

    // // 重置reserved hash，包括stream中的对应值
    #[inline]
    fn set_reserved_hash(&mut self, hash: i64) {
        self.ctx.is_reserved_hash = true;
        self.ctx.reserved_hash = hash;
    }
    // 更新reserved hash
    // #[inline]
    // pub(super) fn update_reserved_hash(&mut self, reserved_hash: i64) {
    //     self.reserved_hash.replace(reserved_hash);
    //     self.stream.ctx.reserved_hash.replace(reserved_hash);
    // }

    // #[inline]
    // pub(super) fn reserved_hash(&self) -> ReservedHash {
    //     self.reserved_hash
    // }

    #[inline]
    pub(super) fn sendto_all(&self) -> bool {
        self.ctx.sendto_all
    }

    #[inline]
    pub(super) fn set_sendto_all(&mut self) {
        self.ctx.sendto_all = true;
    }

    #[inline]
    pub(super) fn clear_status(&mut self, cfg: &CommandProperties) {
        if cfg.effect_on_next_req {
            let master_only = self.master_only();
            let sendto_all = self.sendto_all();
            let is_reserved_hash = self.ctx.is_reserved_hash;
            let reserved_hash = self.ctx.reserved_hash;
            // 重置context、reserved-hash
            self.reset_context();
            self.reserve_status(master_only, sendto_all, is_reserved_hash, reserved_hash);
        } else {
            self.reset_context();
        }
    }

    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(self.oft_last < self.oft, "packet:{}", self);
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;
        // 更新上下文的bulk num。
        self.ctx.first = false;
        *self.stream.context() = self.ctx.into();
        self.stream.take(data.len())
    }

    pub(super) fn flag(&self, cfg: &CommandProperties) -> Flag {
        use super::RedisFlager;
        let mut flag = cfg.flag();
        if self.master_only() {
            flag.set_master_only();
        }
        if self.sendto_all() {
            flag.set_sendto_all();
        }
        flag
    }

    //总是会parsekey
    pub(super) fn hash<H: Hash>(&mut self, cfg: &CommandProperties, alg: &H) -> Result<i64> {
        let mut key: RingSlice = Default::default();
        if cfg.has_key {
            key = self.parse_key()?;
        }
        let hash = if self.ctx.is_reserved_hash {
            self.ctx.reserved_hash
        } else {
            calculate_hash(alg, &key)
        };

        Ok(hash)
    }

    //处理对下条指令有影响的命令，其造成的影响单独存放，不影响平常流程
    pub(super) fn proc_effect_on_next_req_cmd<H: Hash>(
        &mut self,
        cfg: &CommandProperties,
        alg: &H,
    ) -> Result<(Flag, i64)> {
        let hash = 0;
        match cfg.cmd_type {
            CommandType::Master => self.set_master_only(),
            // cmd: hashkeyq $key
            // 流程放到计算hash中处理
            CommandType::SwallowedCmdHashkeyq | CommandType::SpecLocalCmdHashkey => {
                let key = self.parse_key()?;
                //兼容老版本
                if key.len() == 2 && key[0] == ('-' as u8) && key[1] == ('1' as u8) {
                    self.set_sendto_all();
                } else {
                    let hash = calculate_hash(alg, &key);
                    self.set_reserved_hash(hash);
                }
            }
            // cmd: hashrandomq
            CommandType::SwallowedCmdHashrandomq => {
                // 虽然hash名义为i64，但实际当前均为u32
                let hash = rand::random::<u32>() as i64;
                self.set_reserved_hash(hash);
            }
            CommandType::CmdSendToAll | CommandType::CmdSendToAllq => {
                self.set_sendto_all();
            }
            _ => {
                assert!(false, "unknown effect_on_next_req_cmd:{}", cfg.name);
            }
        }
        Ok((cfg.flag(), hash))
    }

    #[inline]
    pub(super) fn reserve_status(
        &mut self,
        master_only: bool,
        sendto_all: bool,
        is_reserved_hash: bool,
        reserved_hash: i64,
    ) {
        if master_only {
            // 保留master only 设置
            self.set_layer(LayerType::MasterOnly);
        }
        if sendto_all {
            self.set_sendto_all()
        }
        self.ctx.is_reserved_hash = is_reserved_hash;
        self.ctx.reserved_hash = reserved_hash;
        *self.stream.context() = self.ctx.into();

        // 设置packet的ctx到stream的ctx中，供下一个指令使用

        // 这个有必要保留吗？
        // if self.available() {
        //     return Ok(());
        // }
        // return Err(crate::Error::ProtocolIncomplete);
    }

    #[inline]
    pub(crate) fn first(&self) -> bool {
        self.ctx.first
    }
    #[inline]
    pub(crate) fn bulk(&self) -> u16 {
        self.ctx.bulk
    }
    #[inline]
    pub(crate) fn op_code(&self) -> u16 {
        self.ctx.op_code
    }
    #[inline]
    pub(crate) fn complete(&self) -> bool {
        self.ctx.bulk == 0
    }
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn inner_data(&self) -> &RingSlice {
        &self.data
    }
    #[inline]
    pub(super) fn set_layer(&mut self, layer: LayerType) {
        self.ctx.layer = layer as u8;
    }
    #[inline]
    pub(super) fn master_only(&self) -> bool {
        self.ctx.layer == LayerType::MasterOnly as u8
    }
    #[inline]
    pub(super) fn set_master_only(&mut self) {
        self.ctx.layer = LayerType::MasterOnly as u8;
    }
    // 解析完毕，如果数据未读完，需要保留足够的buff空间
    #[inline(always)]
    pub(crate) fn reserve_stream_buff(&mut self) {
        if self.oft > self.data.len() {
            log::debug!("+++ will reserve len:{}", (self.oft - self.data.len()));
            self.stream.reserve(self.oft - self.data.len())
        }
    }
}

#[derive(Debug)]
pub struct Packet {
    inner: RingSlice,
}
impl From<RingSlice> for Packet {
    #[inline(always)]
    fn from(s: RingSlice) -> Self {
        Self { inner: s }
    }
}
impl std::ops::Deref for Packet {
    type Target = RingSlice;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

//整体解析原则，解析方保证解析完\r\n, oft移到\n+1, 即作为参数传入的oft不保证未溢出
impl Packet {
    // 调用方确保oft元素为'*'
    // *num\r\n
    // oft移动到\r\n之后。 调用完该方法后，可能出现oft >= self.len()的情况
    #[inline]
    pub fn num_of_bulks(&self, oft: &mut usize) -> crate::Result<usize> {
        debug_assert!(*oft < self.len() && self[*oft] == b'*');
        let mut n = 0;
        for i in *oft + 1..self.len() - 1 {
            if self[i] == b'\r' {
                // 下一个字符必须是'\n'
                debug_assert!(i + 1 >= self.len() || self[i + 1] == b'\n');
                *oft = i + 2;
                return Ok(n);
            }
            debug_assert!(self[i].is_ascii_digit(), "num:{} => {:?}", i, self);
            n = n * 10 + (self[i] - b'0') as usize;
        }
        Err(Error::ProtocolIncomplete)
    }
    // oft + MIN <= self.len()
    // $-1\r\n     ==> null.  返回0
    // $num\r\n 读取num
    // oft移动到\r位置处, 调用方需要处理oft对于\r\n的偏移量
    #[inline]
    pub fn num_of_string(&self, oft: &mut usize) -> Result<usize> {
        debug_assert!(self.check_onetoken(*oft).is_ok(), "{} => {:?}", oft, self);
        debug_assert!(self[*oft] == b'$');
        match self[*oft + 1] {
            b'-' => {
                debug_assert!(self[*oft + 2] == b'1' && self[*oft + 3] == b'\r');
                // 跳过 $-1 3个字节
                // null当前的bulk不包含\r\n，这样使用方可以假设后面带有一个0长度的\r\n
                *oft += 3;
                Ok(0)
            }
            _ => {
                // 解析数字
                let mut n = 0;
                for i in *oft + 1..self.len() {
                    if self[i] == b'\r' {
                        debug_assert!(i + 1 >= self.len() || self[i + 1] == b'\n');
                        // 额外跳过\r\n
                        *oft = i + 2;
                        return Ok(n);
                    }
                    debug_assert!(self[i].is_ascii_digit(), "invalid:{} => {:?}", i, self);
                    n = n * 10 + (self[i] - b'0') as usize;
                }
                Err(Error::ProtocolIncomplete)
            }
        }
    }
    // 第一个字节是类型标识。 '*' '$'等等，由调用方确认。
    // 三种额外的情况处理
    // $0\r\n\r\n  ==> 长度为0的字符串
    // $-1\r\n     ==> null
    // *0\r\n      ==> 0 bulk number
    #[inline]
    pub fn num(&self, oft: &mut usize) -> crate::Result<usize> {
        // 至少4个字节
        if *oft + 4 <= self.len() {
            debug_assert!(
                self[*oft] == b'*' || self[*oft] == b'$',
                "packet:{:?}",
                self
            );
            let start = *oft;
            *oft += NUM_SKIPS[self.at(*oft + 1) as usize] as usize;
            let mut val: usize = 0;
            while *oft < self.len() - 1 {
                let b = self.at(*oft);
                *oft += 1;
                if b == b'\r' {
                    if self.at(*oft) == b'\n' {
                        *oft += 1;
                        if val == 0 {
                            // 如果是长度为$0\r\n\r\n
                            if self.at(start) == b'$' && self.at(start + 1) == b'0' {
                                *oft += 2;
                                if *oft > self.len() {
                                    break;
                                }
                            }
                        }
                        return Ok(val);
                    }
                    // \r后面没有接\n。错误的协议
                    return Err(RedisError::ReqInvalidNoReturn.into());
                }
                if is_number_digit(b) {
                    val = val * 10 + (b - b'0') as usize;
                    if val <= std::u32::MAX as usize {
                        continue;
                    }
                }
                log::info!("oft:{} not valid number:{:?}, {:?}", *oft, self, self);
                return Err(RedisError::ReqInvalidNum.into());
            }
        }
        Err(crate::Error::ProtocolIncomplete)
    }
    #[inline]
    fn num_and_skip(&self, oft: &mut usize) -> crate::Result<usize> {
        let num = self.num(oft)?;
        if num > 0 {
            // skip num个字节 + "\r\n" 2个字节
            *oft += num + 2;
        }
        if *oft <= self.len() {
            Ok(num)
        } else {
            Err(crate::Error::ProtocolIncomplete)
        }
    }
    #[inline(always)]
    pub fn line(&self, oft: &mut usize) -> crate::Result<()> {
        if let Some(idx) = self.find_lf_cr(*oft) {
            *oft = idx + 2;
            Ok(())
        } else {
            Err(crate::Error::ProtocolIncomplete)
        }
    }
    #[inline(always)]
    pub fn check_onetoken(&self, oft: usize) -> Result<()> {
        // 一个token至少4个字节
        if oft + 4 <= self.len() {
            Ok(())
        } else {
            Err(crate::Error::ProtocolIncomplete)
        }
    }
    // 需要支持4种协议格式：（除了-代表的错误类型）
    //    1）* 代表array； 2）$代表bulk 字符串；3）+ 代表简单字符串；4）:代表整型；
    #[inline]
    pub fn skip_all_bulk(&self, oft: &mut usize) -> Result<()> {
        let mut bulk_count = self.num_of_bulks(oft)?;
        // 使用stack实现递归, 通常没有递归，可以初始化这Empty
        let mut levels = Vec::new();
        while bulk_count > 0 || levels.len() > 0 {
            if bulk_count == 0 {
                bulk_count = levels.pop().expect("levels is empty");
            }
            self.check_onetoken(*oft)?;
            match self.at(*oft) {
                b'*' => {
                    let current = self.num_of_bulks(oft)?;
                    if bulk_count > 1 {
                        levels.push(bulk_count - 1);
                    }
                    bulk_count = current;
                    continue;
                }
                b'$' => {
                    // 跳过num个字节 + "\r\n" 2个字节
                    *oft += self.num_of_string(oft)? + CRLF_LEN;
                }
                b'+' | b':' => self.line(oft)?,
                _ => panic!("unsupport rsp:{:?}, pos: {}/{}", self, oft, bulk_count),
            }
            bulk_count -= 1;
        }
        Ok(())
    }
    // 需要支持4种协议格式：（除了-代表的错误类型）
    //    1）* 代表array； 2）$代表bulk 字符串；3）+ 代表简单字符串；4）:代表整型；
    //#[inline]
    //pub fn num_skip_all(&self, oft: &mut usize) -> Result<()> {
    //    let mut bulk_count = self.num(oft)?;
    //    while bulk_count > 0 {
    //        if *oft >= self.len() {
    //            return Err(crate::Error::ProtocolIncomplete);
    //        }
    //        match self.at(*oft) {
    //            b'*' => {
    //                self.num_skip_all(oft)?;
    //            }
    //            b'$' => {
    //                self.num_and_skip(oft)?;
    //            }
    //            b'+' | b':' => self.line(oft)?,
    //            _ => {
    //                log::info!("unsupport rsp:{:?}, pos: {}/{}", self, oft, bulk_count);
    //                panic!("not supported in num_skip_all");
    //            }
    //        }
    //        // data.num_and_skip(&mut oft)?;
    //        bulk_count -= 1;
    //    }
    //    Ok(())
    //}
}
#[inline]
fn is_number_digit(d: u8) -> bool {
    d >= b'0' && d <= b'9'
}

use std::sync::atomic::{AtomicI64, Ordering};
static AUTO: AtomicI64 = AtomicI64::new(0);

// 避免异常情况下hash为0，请求集中到某一个shard上。
// hash正常情况下可能为0?
#[inline]
fn calculate_hash<H: Hash>(alg: &H, key: &RingSlice) -> i64 {
    match key.len() {
        0 => default_hash(),
        // 2 => {
        //     // 对“hashkey -1”做特殊处理，使用max hash，从而保持与hashkeyq一致
        //     if key.len() == 2
        //         && key.at(0) == ('-' as u8)
        //         && key.at(1) == ('1' as u8)
        //         && cfg.cmd_type == CommandType::SpecLocalCmdHashkey
        //     {
        //         crate::MAX_DIRECT_HASH
        //     } else {
        //         alg.hash(key)
        //     }
        // }
        _ => alg.hash(key),
    }
    // if key.len() == 0 {
    //     default_hash()
    // } else {
    //     alg.hash(key)
    // }
}

#[inline]
fn default_hash() -> i64 {
    AUTO.fetch_add(1, Ordering::Relaxed)
}

use std::fmt::{self, Debug, Display, Formatter};

impl<'a, S: crate::Stream> Display for RequestPacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(packet => len:{} bulk num: {} op_code:{} oft:({} => {})) first:{} data:{:?}",
            self.data.len(),
            self.bulk(),
            self.op_code(),
            self.oft_last,
            self.oft,
            self.first(),
            self.data
        )
    }
}
impl<'a, S: crate::Stream> Debug for RequestPacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

// 在Packet::num时，需要跳过第一个symbol(可能是*也可能是$)，如果下一个字符是
// '-'，则说明当前的num是0，则需要3个字节，即 "$-1".  格式为 $-1\r\n
// '0'，则说明格式为 $0\r\n\r\n. 需跳过4字节，"$0\r\n"。
// 其他只需要跳过 $或者*这一个字节即可。
const NUM_SKIPS: [u8; 256] = [
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 3, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
];
