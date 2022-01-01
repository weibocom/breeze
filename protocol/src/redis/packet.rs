pub(super) trait Packet {
    // 从oft指定的位置开始，解析数字，直到\r\n。
    // 协议错误返回Err
    // 如果数据不全，则返回ProtocolIncomplete
    // 1. number; 2. 返回下一次扫描的oft的位置
    fn num(&self, oft: &mut usize) -> crate::Result<usize>;
    fn line(&self, oft: &mut usize) -> crate::Result<()>;
}

impl Packet for ds::RingSlice {
    // 第一个字节是类型标识。 '*' '$'等等，由调用方确认。
    #[inline(always)]
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
