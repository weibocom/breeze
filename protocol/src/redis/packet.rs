use crate::{Error, Result};
use ds::RingSlice;

const CRLF_LEN: usize = b"\r\n".len();

// 这个context是用于中multi请求中，同一个multi请求中跨request协调
// 必须是u64长度的。
#[repr(C)]
#[derive(Debug)]
struct RequestContext {
    bulk: u16,
    op_code: u16,
    _ignore: u32,
}
impl RequestContext {
    #[inline(always)]
    fn reset(&mut self) {
        debug_assert_eq!(std::mem::size_of::<Self>(), 8);
        *self.u64_mut() = 0;
    }
    #[inline(always)]
    fn u64(&mut self) -> u64 {
        *self.u64_mut()
    }
    #[inline(always)]
    fn u64_mut(&mut self) -> &mut u64 {
        unsafe { std::mem::transmute(self) }
    }
    #[inline(always)]
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
    pub(super) first: bool,
}
impl<'a, S: crate::Stream> RequestPacket<'a, S> {
    #[inline(always)]
    pub(super) fn new(stream: &'a mut S) -> Self {
        let ctx = RequestContext::from(*stream.context());
        let data = stream.slice();
        Self {
            oft_last: 0,
            oft: 0,
            data,
            first: ctx.bulk == 0,
            ctx,
            stream,
        }
    }
    #[inline(always)]
    pub(super) fn has_bulk(&self) -> bool {
        self.bulk() > 0
    }
    #[inline(always)]
    pub(super) fn available(&self) -> bool {
        self.oft < self.data.len()
    }
    #[inline(always)]
    pub(super) fn parse_bulk_num(&mut self) -> Result<()> {
        if self.bulk() == 0 {
            self.check_start()?;
            self.ctx.bulk = self.data.num(&mut self.oft)? as u16;
            debug_assert_ne!(self.bulk(), 0);
        }
        Ok(())
    }
    #[inline(always)]
    pub(super) fn parse_cmd(&mut self) -> Result<()> {
        // 需要确保，如果op_code不为0，则之前的数据一定处理过。
        if self.ctx.op_code == 0 {
            let cmd_len = self.data.num_and_skip(&mut self.oft)?;
            self.ctx.bulk -= 1;
            let start = self.oft - cmd_len - CRLF_LEN;
            let cmd = self.data.sub_slice(start, cmd_len);
            self.ctx.op_code = super::command::get_op_code(&cmd);
            debug_assert_ne!(self.ctx.op_code, 0);
        }
        Ok(())
    }
    #[inline(always)]
    pub(super) fn parse_key(&mut self) -> Result<RingSlice> {
        debug_assert_ne!(self.ctx.op_code, 0);
        debug_assert_ne!(self.ctx.bulk, 0);
        let key_len = self.data.num_and_skip(&mut self.oft)?;
        self.ctx.bulk -= 1;
        let start = self.oft - CRLF_LEN - key_len;
        Ok(self.data.sub_slice(start, key_len))
    }
    #[inline(always)]
    pub(super) fn ignore_one_bulk(&mut self) -> Result<()> {
        debug_assert_ne!(self.ctx.bulk, 0);
        self.data.num_and_skip(&mut self.oft)?;
        self.ctx.bulk -= 1;
        Ok(())
    }
    #[inline(always)]
    pub(super) fn ignore_all_bulks(&mut self) -> Result<()> {
        while self.bulk() > 0 {
            self.ignore_one_bulk()?;
        }
        Ok(())
    }
    // 忽略掉之前的数据，通常是multi请求的前面部分。
    #[inline(always)]
    pub(super) fn multi_ready(&mut self) {
        if self.oft > self.oft_last {
            self.stream.ignore(self.oft - self.oft_last);
            self.oft_last = self.oft;

            debug_assert_ne!(self.ctx.op_code, 0);
            // 更新
            *self.stream.context() = self.ctx.u64();
        }
    }
    #[inline(always)]
    pub(super) fn take(&mut self) -> ds::MemGuard {
        debug_assert!(self.oft_last < self.oft);
        let data = self.data.sub_slice(self.oft_last, self.oft - self.oft_last);
        self.oft_last = self.oft;
        self.first = false;
        // 更新上下文的bulk num。
        *self.stream.context() = self.ctx.u64();
        if self.ctx.bulk == 0 {
            self.ctx.reset();
            debug_assert_eq!(self.ctx.u64(), 0);
            *self.stream.context() = 0;
        }
        self.stream.take(data.len())
    }
    #[inline(always)]
    fn current(&self) -> u8 {
        debug_assert!(self.available());
        self.data.at(self.oft)
    }
    #[inline(always)]
    fn check_start(&self) -> Result<()> {
        if self.current() != b'*' {
            Err(Error::RequestProtocolNotValidStar)
        } else {
            Ok(())
        }
    }
    #[inline(always)]
    pub(super) fn bulk(&self) -> u16 {
        self.ctx.bulk
    }
    #[inline(always)]
    pub(super) fn op_code(&self) -> u16 {
        self.ctx.op_code
    }
    #[inline(always)]
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
    #[inline]
    fn num(&self, oft: &mut usize) -> crate::Result<usize> {
        if *oft + 2 < self.len() {
            debug_assert!(is_valid_leading_num_char(self.at(*oft)));
            *oft += 1;
            let mut val: usize = 0;
            while *oft < self.len() - 1 {
                let b = self.at(*oft);
                *oft += 1;
                if b == b'\r' {
                    if self.at(*oft) == b'\n' {
                        *oft += 1;
                        return Ok(val);
                    }
                    // \r后面没有接\n。错误的协议
                    return Err(crate::Error::RequestProtocolNotValidNoReturn);
                }
                if is_number_digit(b) {
                    val = val * 10 + (b - b'0') as usize;
                    if val <= std::u32::MAX as usize {
                        continue;
                    }
                }
                return Err(crate::Error::RequestProtocolNotValidNumber);
            }
        }
        Err(crate::Error::ProtocolIncomplete)
    }
    #[inline]
    fn num_and_skip(&self, oft: &mut usize) -> crate::Result<usize> {
        let num = self.num(oft)?;
        *oft += num + 2;
        if *oft <= self.len() {
            Ok(num)
        } else {
            Err(crate::Error::ProtocolIncomplete)
        }
    }
    #[inline(always)]
    fn line(&self, oft: &mut usize) -> crate::Result<()> {
        if let Some(idx) = self.find_lf_cr(*oft) {
            *oft = idx + 2;
            Ok(())
        } else {
            Err(crate::Error::ProtocolIncomplete)
        }
    }
}
#[inline(always)]
fn is_number_digit(d: u8) -> bool {
    d >= b'0' && d <= b'9'
}
// 这个字节后面会带一个数字。
#[inline(always)]
fn is_valid_leading_num_char(d: u8) -> bool {
    d == b'$' || d == b'*'
}
use std::fmt::{self, Debug, Display, Formatter};
impl<'a, S: crate::Stream> Display for RequestPacket<'a, S> {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(packet => stream len:{} parsing bulk num: {} op_code:{} oft:({} => {})) first:{} ",
            self.data.len(),
            self.bulk(),
            self.op_code(),
            self.oft_last,
            self.oft,
            self.first,
        )
    }
}
impl<'a, S: crate::Stream> Debug for RequestPacket<'a, S> {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}
