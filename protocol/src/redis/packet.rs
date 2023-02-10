use super::{
    command::{CommandProperties, Commands},
    error::RedisError,
};
use crate::{HashedCommand, Result};
use ds::{MemGuard, RingSlice};

const CRLF_LEN: usize = b"\r\n".len();
// 这个context是用于中multi请求中，同一个multi请求中跨request协调
// 必须是u64长度的。
#[repr(C)]
#[derive(Debug)]
pub struct RequestContext {
    bulk: u16,
    op_code: u16,
    first: bool, // 在multi-get请求中是否是第一个请求。
    layer: u8,   // 请求的层次，目前只支持：master，all
    _ignore: [u8; 2],
}

// 请求的layer层次，目前只有masterOnly，后续支持业务访问某层时，在此扩展属性
pub enum LayerType {
    MasterOnly = 1,
}

impl RequestContext {
    #[inline]
    fn reset(&mut self) {
        assert_eq!(std::mem::size_of::<Self>(), 8);
        *self.u64_mut() = 0;
    }
    #[inline]
    fn u64(&mut self) -> u64 {
        *self.u64_mut()
    }
    #[inline]
    fn u64_mut(&mut self) -> &mut u64 {
        unsafe { std::mem::transmute(self) }
    }
    #[inline]
    fn from(v: u64) -> Self {
        unsafe { std::mem::transmute(v) }
    }
}

//包含流解析过程中当前命令和前面命令的状态
pub(crate) struct RequestPacket<'a, S> {
    stream: &'a mut S,
    data: RingSlice,
    // 低16位是bulk_num
    // 次低16位是op_code.
    ctx: RequestContext,
    reserved_hash: i64,
    oft_last: usize,
    oft: usize,
}

impl<'a, S: crate::Stream> RequestPacket<'a, S> {
    #[inline]
    pub(crate) fn new(stream: &'a mut S) -> Self {
        let ctx = RequestContext::from(*stream.context());
        let reserved_hash = *stream.reserved_hash();
        let data = stream.slice();
        Self {
            oft_last: 0,
            oft: 0,
            data,
            ctx,
            reserved_hash,
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
            self.check_start()?;
            self.ctx.bulk = self.data.num(&mut self.oft)? as u16;
            self.ctx.first = true;
            assert_ne!(self.bulk(), 0, "packet:{}", self);
        }
        Ok(())
    }
    #[inline]
    pub(crate) fn parse_cmd(
        &mut self,
        cmds: &'static Commands,
    ) -> Result<&'static CommandProperties> {
        let cfg;
        // 需要确保，如果op_code不为0，则之前的数据一定处理过。
        if self.ctx.op_code == 0 {
            let cmd_len = self.data.num_and_skip(&mut self.oft)?;
            let start = self.oft - cmd_len - CRLF_LEN;
            let cmd = self.data.sub_slice(start, cmd_len);
            self.ctx.op_code = cmds.get_op_code(&cmd);
            assert_ne!(self.ctx.op_code, 0, "packet:{}", self);

            // 第一次解析cmd需要对协议进行合法性校验
            cfg = cmds.get_by_op(self.op_code())?;
            cfg.validate(self.bulk() as usize)?;

            // cmd name 解析完毕，bulk 减 1
            self.ctx.bulk -= 1;
        } else {
            cfg = cmds.get_by_op(self.ctx.op_code)?;
        }
        Ok(cfg)
    }
    #[inline]
    pub(crate) fn build_request_with_key(
        &self,
        cfg: &CommandProperties,
        hash: i64,
        real_key: &RingSlice,
    ) -> HashedCommand {
        use ds::Buffer;
        assert!(cfg.name.len() < 10, "name:{}", cfg.name);
        let mut cmd = Vec::with_capacity(24 + real_key.len());
        cmd.push(b'*');
        // 1个cmd, 1个key，1个value。一共3个bulk
        cmd.push((2 + cfg.has_val as u8) + b'0');
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(cfg.mname.len().to_string());
        cmd.write("\r\n");
        cmd.write(cfg.mname);
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(real_key.len().to_string());
        cmd.write("\r\n");
        cmd.write_slice(real_key);
        cmd.write("\r\n");

        //data.copy_to_vec(&mut cmd);
        let flag = cfg.flag();
        let cmd: MemGuard = MemGuard::from_vec(cmd);
        HashedCommand::new(cmd, hash, flag)
    }

    // bulk_num只有在first=true时才有意义。
    #[inline]
    pub(crate) fn build_request_with_key_for_multi(
        &self,
        cfg: &CommandProperties,
        hash: i64,
        bulk_num: u16,
        first: bool,
        real_key: &RingSlice,
    ) -> HashedCommand {
        use ds::Buffer;
        assert!(cfg.name.len() < 10, "name:{}", cfg.name);
        let mut cmd = Vec::with_capacity(24 + real_key.len());
        cmd.push(b'*');
        // 1个cmd, 1个key，1个value。一共3个bulk
        cmd.push((2 + cfg.has_val as u8) + b'0');
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(cfg.mname.len().to_string());
        cmd.write("\r\n");
        cmd.write(cfg.mname);
        cmd.write("\r\n");
        cmd.push(b'$');
        cmd.write(real_key.len().to_string());
        cmd.write("\r\n");
        cmd.write_slice(real_key);
        cmd.write("\r\n");

        //data.copy_to_vec(&mut cmd);
        let mut flag = cfg.flag();
        use super::flag::RedisFlager;
        if first {
            flag.set_mkey_first();
            // mset只有一个返回值。
            // 其他的multi请求的key的数量就是bulk_num
            assert!(
                cfg.key_step == 1 || cfg.key_step == 2,
                "step:{}",
                cfg.key_step
            );
            let mut key_num = bulk_num;
            if cfg.key_step == 2 {
                key_num >>= 1;
            }
            flag.set_key_count(key_num);
        }
        let cmd: MemGuard = MemGuard::from_vec(cmd);
        HashedCommand::new(cmd, hash, flag)
    }
    #[inline]
    pub(crate) fn parse_key(&mut self) -> Result<RingSlice> {
        assert_ne!(self.ctx.op_code, 0, "packet:{:?}", self);
        assert_ne!(self.ctx.bulk, 0, "packet:{:?}", self);
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
            *self.stream.context() = self.ctx.u64();
        }
    }

    // 重置context，包括stream中的context
    #[inline]
    fn reset_context(&mut self) {
        // 重置packet的ctx
        self.ctx.reset();

        // 重置stream的ctx
        *self.stream.context() = 0;

        assert_eq!(self.ctx.u64(), 0, "packet:{}", self);
    }

    // 重置reserved hash，包括stream中的对应值
    #[inline]
    fn reset_reserved_hash(&mut self) {
        self.update_reserved_hash(0)
    }
    // 更新reserved hash
    #[inline]
    pub(super) fn update_reserved_hash(&mut self, reserved_hash: i64) {
        self.reserved_hash = reserved_hash;
        *self.stream.reserved_hash() = reserved_hash;
    }

    #[inline]
    pub(super) fn reserved_hash(&self) -> i64 {
        self.reserved_hash
    }

    #[inline]
    pub(crate) fn take(&mut self) -> ds::MemGuard {
        assert!(self.oft_last < self.oft, "packet:{}", self);
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;
        // 更新上下文的bulk num。
        self.ctx.first = false;
        *self.stream.context() = self.ctx.u64();
        if self.ctx.bulk == 0 {
            // 重置context、reserved-hash
            self.reset_context();
            self.reset_reserved_hash();
        }
        self.stream.take(data.len())
    }

    // trim掉已解析的cmd相关元数据，只保留在master_only、reserved_hash这两个元数据
    #[inline]
    pub(super) fn trim_swallowed_cmd(&mut self) -> Result<()> {
        // trim 吞噬指令时，整个吞噬指令必须已经被解析完毕
        debug_assert!(self.ctx.bulk == 0, "packet:{}", self);

        // 移动oft到吞噬指令之后
        let len = self.oft - self.oft_last;
        self.oft_last = self.oft;
        self.stream.ignore(len);

        // 记录需保留的状态：目前只有master only状态【direct hash保存在stream的非ctx字段中】
        let master_only = self.master_only();

        // 重置context，去掉packet/stream的ctx信息
        self.reset_context();

        // 保留后续cmd执行需要的状态：当前只有master
        if master_only {
            // 保留master only 设置
            self.set_layer(LayerType::MasterOnly);
        }

        // 设置packet的ctx到stream的ctx中，供下一个指令使用
        *self.stream.context() = self.ctx.u64();

        if self.available() {
            return Ok(());
        }
        return Err(crate::Error::ProtocolIncomplete);
    }

    #[inline]
    fn current(&self) -> u8 {
        assert!(self.available(), "packet:{}", self);
        self.data.at(self.oft)
    }
    #[inline]
    fn check_start(&self) -> Result<()> {
        if self.current() != b'*' {
            Err(RedisError::ReqInvalidStar.error())
        } else {
            Ok(())
        }
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

    // 解析完毕，如果数据未读完，需要保留足够的buff空间
    #[inline(always)]
    pub(crate) fn reserve_stream_buff(&mut self) {
        if self.oft > self.stream.len() {
            log::debug!("+++ will reserve len:{}", (self.oft - self.stream.len()));
            self.stream.reserve(self.oft - self.data.len())
        }
    }
}

pub trait Packet {
    // 从oft指定的位置开始，解析数字，直到\r\n。
    // 协议错误返回Err
    // 如果数据不全，则返回ProtocolIncomplete
    // 1. number; 2. 返回下一次扫描的oft的位置
    fn num(&self, oft: &mut usize) -> crate::Result<usize>;
    // 计算当前的数字，并且将其跳过。如果跑过的字节数比计算的num少，则返回ProtocolIncomplete
    fn num_and_skip(&self, oft: &mut usize) -> crate::Result<usize>;
    fn line(&self, oft: &mut usize) -> crate::Result<()>;
    fn num_skip_all(&self, oft: &mut usize) -> Result<()>;
}

impl Packet for ds::RingSlice {
    // 第一个字节是类型标识。 '*' '$'等等，由调用方确认。
    // 三种额外的情况处理
    // $0\r\n\r\n  ==> 长度为0的字符串
    // $-1\r\n     ==> null
    // *0\r\n      ==> 0 bulk number
    #[inline]
    fn num(&self, oft: &mut usize) -> crate::Result<usize> {
        if *oft + 2 < self.len() {
            assert!(is_valid_leading_num_char(self.at(*oft)), "packet:{}", self);
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
                    return Err(RedisError::ReqInvalidNoReturn.error());
                }
                if is_number_digit(b) {
                    val = val * 10 + (b - b'0') as usize;
                    if val <= std::u32::MAX as usize {
                        continue;
                    }
                }
                log::info!("oft:{} not valid number:{:?}, {:?}", *oft, self, self);
                return Err(RedisError::ReqInvalidNum.error());
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
    #[inline]
    fn line(&self, oft: &mut usize) -> crate::Result<()> {
        if let Some(idx) = self.find_lf_cr(*oft) {
            *oft = idx + 2;
            Ok(())
        } else {
            Err(crate::Error::ProtocolIncomplete)
        }
    }
    // 需要支持4种协议格式：（除了-代表的错误类型）
    //    1）* 代表array； 2）$代表bulk 字符串；3）+ 代表简单字符串；4）:代表整型；
    #[inline]
    fn num_skip_all(&self, oft: &mut usize) -> Result<()> {
        let mut bulk_count = self.num(oft)?;
        while bulk_count > 0 {
            if *oft >= self.len() {
                return Err(crate::Error::ProtocolIncomplete);
            }
            match self.at(*oft) {
                b'*' => {
                    self.num_skip_all(oft)?;
                }
                b'$' => {
                    self.num_and_skip(oft)?;
                }
                b'+' => self.line(oft)?,
                b':' => self.line(oft)?,
                _ => {
                    log::info!("unsupport rsp:{:?}, pos: {}/{}", self, oft, bulk_count);
                    panic!("not supported in num_skip_all");
                }
            }
            // data.num_and_skip(&mut oft)?;
            bulk_count -= 1;
        }
        Ok(())
    }
}
#[inline]
fn is_number_digit(d: u8) -> bool {
    d >= b'0' && d <= b'9'
}
// 这个字节后面会带一个数字。
#[inline]
fn is_valid_leading_num_char(d: u8) -> bool {
    d == b'$' || d == b'*'
}
#[allow(dead_code)]
fn num_inner(data: &RingSlice, oft: &mut usize) -> crate::Result<usize> {
    let mut val = 0;
    while *oft < data.len() - 1 {
        let b = data.at(*oft);
        *oft += 1;
        if b == b'\r' {
            if data.at(*oft) == b'\n' {
                *oft += 1;
                return Ok(val);
            }
            // \r后面没有接\n。错误的协议
            return Err(RedisError::ReqInvalidNoReturn.error());
        }
        if is_number_digit(b) {
            val = val * 10 + (b - b'0') as usize;
            if val <= std::u32::MAX as usize {
                continue;
            }
        }
        return Err(RedisError::ReqInvalidNum.error());
    }
    Err(crate::Error::ProtocolIncomplete)
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
