use crate::Result;
use ds::RingSlice;

const CRLF_LEN: usize = b"\r\n".len();

// 这个context是用于中multi请求中，同一个multi请求中跨request协调
// 必须是u64长度的。
#[repr(C)]
#[derive(Debug)]
struct RequestContext {
    bulk: u16,
    op_code: u16,
    first: bool, // 在multi-get请求中是否是第一个请求。
    _ignore: [u8; 3],
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
    oft_last: usize,
    oft: usize,
}
impl<'a, S: crate::Stream> RequestPacket<'a, S> {
    #[inline]
    pub(super) fn new(stream: &'a mut S) -> Self {
        let ctx = RequestContext::from(*stream.context());
        let data = stream.slice();
        Self {
            oft_last: 0,
            oft: 0,
            data,
            ctx,
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
    #[inline]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        assert!(self.oft_last < self.oft);
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;
        // 更新上下文的bulk num。
        self.ctx.first = false;
        *self.stream.context() = self.ctx.u64();
        if self.ctx.bulk == 0 {
            self.ctx.reset();
            assert_eq!(self.ctx.u64(), 0);
            *self.stream.context() = 0;
        }
        self.stream.take(data.len())
    }
    #[inline]
    fn current(&self) -> u8 {
        assert!(self.available());
        self.data.at(self.oft)
    }
    #[inline]
    fn check_start(&self) -> Result<()> {
        if self.current() != b'*' {
            Err(PtError::ReqInvalid.error())
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
                    return Err(PtError::ReqInvalidNoReturn.error());
                }
                if is_number_digit(b) {
                    val = val * 10 + (b - b'0') as usize;
                    if val <= std::u32::MAX as usize {
                        continue;
                    }
                }
                use crate::Utf8;
                log::info!(
                    "oft:{} not valid number:{:?}, {:?}",
                    *oft,
                    self.utf8(),
                    self
                );
                return Err(PtError::ReqInvalidNoReturn.error());
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
            return Err(PtError::ReqInvalidNoReturn.error());
        }
        if is_number_digit(b) {
            val = val * 10 + (b - b'0') as usize;
            if val <= std::u32::MAX as usize {
                continue;
            }
        }
        return Err(PtError::ReqInvalidNum.error());
    }
    Err(crate::Error::ProtocolIncomplete)
}

use std::fmt::{self, Debug, Display, Formatter};

use super::error::PtError;
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
