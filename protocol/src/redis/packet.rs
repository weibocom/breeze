pub(super) trait Packet {
    // 从oft指定的位置开始，解析数字，直到\r\n。
    // 协议错误返回Err
    // 如果数据不全，则返回ProtocolIncomplete
    // 1. number; 2. 返回下一次扫描的oft的位置
    fn num(&self, oft: &mut usize) -> crate::Result<usize>;
    fn line(&self, oft: &mut usize) -> crate::Result<()>;
}

impl Packet for ds::RingSlice {
    #[inline(always)]
    fn num(&self, oft: &mut usize) -> crate::Result<usize> {
        if *oft + 1 < self.len() {
            let mut val: usize = 0;
            while *oft < self.len() - 1 {
                let b = self.at(*oft);
                *oft += 1;
                if b == b'\r' {
                    break;
                }
                if b > b'9' || b < b'0' {
                    return Err(crate::Error::RequestProtocolNotValidDigit);
                }
                val = val * 10 + (b - b'0') as usize;
                if val >= std::u32::MAX as usize {
                    return Err(crate::Error::RequestProtocolNotValidNumberOverFlow);
                }
            }

            if self.at(*oft) == b'\n' {
                *oft += 1;
                return Ok(val);
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
