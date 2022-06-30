use crate::{Error, Result, Utf8};
use ds::RingSlice;

const CRLF_LEN: usize = b"\r\n".len();
// 防止client发大小写组合的master，需要忽略master的大小写 fishermen
const MASTER_CMD: &str = "*1\r\n$6\r\nMASTER\r\n";

// 这个context是用于中multi请求中，同一个multi请求中跨request协调
// 必须是u64长度的。
#[repr(C)]
#[derive(Debug)]
struct RequestContext {
    bulk: u16,
    op_code: u16,
    first: bool, // 在multi-get请求中是否是第一个请求。
    layer: u8,   // 请求的层次，目前只支持：master，all
    _ignore: [u8; 2],
}

// 请求的layer层次
pub enum LayerType {
    NotChecked = 0,
    All = 1,
    MasterOnly = 2,
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

pub(super) struct RequestPacket<'a, S> {
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
    pub(super) fn new(stream: &'a mut S) -> Self {
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
    pub(super) fn has_bulk(&self) -> bool {
        self.bulk() > 0
    }
    #[inline]
    pub(super) fn available(&self) -> bool {
        self.oft < self.data.len()
    }

    // 解析访问layer，由于master非独立指令，如果返回Ok，需要确保还有数据可以继续解析
    // master 过于简单，先直接解析处理，后续可以加到swallowed cmd中
    #[inline]
    pub(super) fn parse_cmd_layer(&mut self) -> Result<bool> {
        if !self.layer_checked() {
            match self
                .data
                .start_with_ignore_case(self.oft, MASTER_CMD.as_bytes())
            {
                Ok(rs) => {
                    if rs {
                        self.oft += MASTER_CMD.len();
                        self.oft_last = self.oft;
                        self.stream.ignore(MASTER_CMD.len());
                        self.set_layer(LayerType::MasterOnly);

                        // master 不是独立指令，只有还有数据可解析时，才返回Ok，否则协议不完整 fishermen
                        if self.available() {
                            return Ok(true);
                        }
                        return Err(Error::ProtocolIncomplete);
                    } else {
                        self.set_layer(LayerType::All);
                        return Ok(false);
                    }
                }
                // 只有长度不够才会返回err
                Err(_) => return Err(Error::ProtocolIncomplete),
            };
        }
        // 之前已经check过，直接取
        Ok(self.master_only())
    }
    #[inline]
    pub(super) fn parse_bulk_num(&mut self) -> Result<()> {
        if self.bulk() == 0 {
            self.check_start()?;
            self.ctx.bulk = self.data.num(&mut self.oft)? as u16;
            self.ctx.first = true;
            assert_ne!(self.bulk(), 0);
        }
        Ok(())
    }
    #[inline]
    pub(super) fn parse_cmd(&mut self) -> Result<()> {
        // 需要确保，如果op_code不为0，则之前的数据一定处理过。
        if self.ctx.op_code == 0 {
            let cmd_len = self.data.num_and_skip(&mut self.oft)?;
            self.ctx.bulk -= 1;
            let start = self.oft - cmd_len - CRLF_LEN;
            let cmd = self.data.sub_slice(start, cmd_len);
            self.ctx.op_code = super::command::get_op_code(&cmd);
            assert_ne!(self.ctx.op_code, 0);
        }
        Ok(())
    }
    #[inline]
    pub(super) fn parse_key(&mut self) -> Result<RingSlice> {
        assert_ne!(self.ctx.op_code, 0);
        assert_ne!(self.ctx.bulk, 0);
        let key_len = self.data.num_and_skip(&mut self.oft)?;
        self.ctx.bulk -= 1;
        let start = self.oft - CRLF_LEN - key_len;
        Ok(self.data.sub_slice(start, key_len))
    }
    #[inline]
    pub(super) fn ignore_one_bulk(&mut self) -> Result<()> {
        assert_ne!(self.ctx.bulk, 0);
        self.data.num_and_skip(&mut self.oft)?;
        self.ctx.bulk -= 1;
        Ok(())
    }
    #[inline]
    pub(super) fn ignore_all_bulks(&mut self) -> Result<()> {
        while self.bulk() > 0 {
            self.ignore_one_bulk()?;
        }
        Ok(())
    }
    // 忽略掉之前的数据，通常是multi请求的前面部分。
    #[inline]
    pub(super) fn multi_ready(&mut self) {
        if self.oft > self.oft_last {
            self.stream.ignore(self.oft - self.oft_last);
            self.oft_last = self.oft;

            assert_ne!(self.ctx.op_code, 0);
            // 更新
            *self.stream.context() = self.ctx.u64();
        }
    }

    // 重置context，包括stream中的context
    #[inline]
    fn reset_context(&mut self) {
        self.ctx.reset();
        assert_eq!(self.ctx.u64(), 0);

        // 重置context
        *self.stream.context() = 0;
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
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(self.oft_last < self.oft);
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

    // 修建掉已解析的cmd数据，这些cmd数据的信息已经保留在context、reserved_hash等元数据中了
    #[inline]
    pub(super) fn trim_cmd_data(&mut self) -> Result<()> {
        let len = self.oft - self.oft_last;
        self.oft_last = self.oft;
        self.stream.ignore(len);

        debug_assert!(self.ctx.bulk == 0);
        // 重置context，下一个指令重新解析，注意：master_only要保留
        let master_only = self.master_only();
        self.reset_context();
        if master_only {
            // 保留master only 设置
            self.set_layer(LayerType::MasterOnly);
        }

        if self.available() {
            return Ok(());
        }
        return Err(crate::Error::ProtocolIncomplete);
    }

    #[inline]
    fn current(&self) -> u8 {
        assert!(self.available());
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
    pub(super) fn first(&self) -> bool {
        self.ctx.first
    }
    #[inline]
    pub(super) fn bulk(&self) -> u16 {
        self.ctx.bulk
    }
    #[inline]
    pub(super) fn op_code(&self) -> u16 {
        self.ctx.op_code
    }
    #[inline]
    pub(super) fn complete(&self) -> bool {
        self.ctx.bulk == 0
    }
    #[inline]
    pub(super) fn inner_data(&self) -> &RingSlice {
        &self.data
    }
    #[inline]
    pub fn layer_checked(&self) -> bool {
        self.ctx.layer != (LayerType::NotChecked as u8)
    }
    #[inline]
    pub(super) fn set_layer(&mut self, layer: LayerType) {
        self.ctx.layer = layer as u8;
        *self.stream.context() = self.ctx.u64();
    }
    #[inline]
    pub(super) fn master_only(&self) -> bool {
        self.ctx.layer == LayerType::MasterOnly as u8
    }
}

pub(super) trait Packet {
    // 从oft指定的位置开始，解析数字，直到\r\n。
    // 协议错误返回Err
    // 如果数据不全，则返回ProtocolIncomplete
    // 1. number; 2. 返回下一次扫描的oft的位置
    fn num(&self, oft: &mut usize) -> crate::Result<usize>;
    // 计算当前的数字，并且将其跳过。如果跑过的字节数比计算的num少，则返回ProtocolIncomplete
    fn num_and_skip(&self, oft: &mut usize) -> crate::Result<usize>;
    fn line(&self, oft: &mut usize) -> crate::Result<()>;
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
            assert!(is_valid_leading_num_char(self.at(*oft)));
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
                log::info!(
                    "oft:{} not valid number:{:?}, {:?}",
                    *oft,
                    self.utf8(),
                    self
                );
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

use super::error::RedisError;
impl<'a, S: crate::Stream> Display for RequestPacket<'a, S> {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(packet => len:{} bulk num: {} op_code:{} oft:({} => {})) first:{}",
            self.data.len(),
            self.bulk(),
            self.op_code(),
            self.oft_last,
            self.oft,
            self.first()
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
