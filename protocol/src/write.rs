use super::Result;
pub trait Writer: ds::BufWriter + Sized {
    fn cap(&self) -> usize;
    fn pending(&self) -> usize;
    // 写数据，一次写完
    fn write(&mut self, data: &[u8]) -> Result<()>;
    #[inline(always)]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.write(&[v])
    }
    // 按str类型写入数字
    // write(v.to_string().as_bytes())
    #[inline]
    fn write_s_u16(&mut self, v: u16) -> Result<()> {
        match v {
            0..=9 => self.write_u8(b'0' + v as u8),
            10..=99 => {
                let mut buf = [0u8; 2];
                buf[0] = b'0' + (v / 10) as u8;
                buf[1] = b'0' + (v % 10) as u8;
                self.write(&buf)
            }
            100..=u16::MAX => {
                let mut buf = [0u8; 8];
                let mut left = v;
                let mut idx = buf.len();
                while left > 0 {
                    idx -= 1;
                    buf[idx] = (left % 10) as u8 + b'0';
                    left = left / 10;
                }
                self.write(&buf[idx..])
            }
        }
        //if v < ds::NUM_STR_TBL.len() as u16 {
        //    self.write(ds::NUM_STR_TBL[v as usize].as_bytes())
        //} else {
        //    self.write(v.to_string().as_bytes())
        //}
    }

    // hint: 提示可能优先写入到cache
    fn cache(&mut self, hint: bool);

    #[inline]
    fn write_slice(&mut self, data: &ds::RingSlice, oft: usize) -> Result<()> {
        data.copy_to(oft, self)?;
        //let mut oft = oft;
        //let len = data.len();
        //log::debug!("+++ will write to client/server:{:?}", data);
        //while oft < len {
        //    let data = data.read(oft);
        //    oft += data.len();
        //    if oft < len {
        //        // 说明有多次写入，将其cache下来
        //        self.cache(true);
        //    }
        //    self.write(data)?;
        //}
        Ok(())
    }
    fn shrink(&mut self);
}
//impl Writer for Vec<u8> {
//    #[inline]
//    fn cap(&self) -> usize {
//        self.capacity()
//    }
//    #[inline]
//    fn pending(&self) -> usize {
//        self.len()
//    }
//    #[inline]
//    fn write(&mut self, data: &[u8]) -> Result<()> {
//        ds::vec::Buffer::write(self, data);
//        Ok(())
//    }
//    #[inline]
//    fn write_u8(&mut self, v: u8) -> Result<()> {
//        self.push(v);
//        Ok(())
//    }
//    #[inline]
//    fn shrink(&mut self) {
//        todo!("should not call shrink");
//    }
//}
