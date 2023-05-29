use byteorder::LittleEndian;

use super::Result;
pub trait Writer: ds::BufWriter + Sized {
    fn cap(&self) -> usize;
    fn pending(&self) -> usize;
    // 写数据，一次写完
    fn write(&mut self, data: &[u8]) -> Result<()>;
    #[inline]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.write(&[v])
    }
    #[inline]
    fn write_u16(&mut self, v: u16) -> Result<()> {
        // let mut data = Vec::with_capacity(2);
        // data.write_u16::<BigEndian>(v)?;
        // self.write(&data[0..])
        self.write(&v.to_be_bytes())
    }
    #[inline]
    fn write_u32(&mut self, v: u32) -> Result<()> {
        // let mut data = Vec::with_capacity(4);
        // data.write_u32::<BigEndian>(v)?;
        // self.write(&data[0..])
        self.write(&v.to_be_bytes())
    }
    #[inline]
    fn write_u64(&mut self, v: u64) -> Result<()> {
        // let mut data = Vec::with_capacity(8);
        // data.write_u64::<BigEndian>(v)?;
        // self.write(&data[0..])
        self.write(&v.to_be_bytes())
    }
    // 按str类型写入数字
    // write(v.to_string().as_bytes())
    #[inline]
    fn write_s_u16(&mut self, v: u16) -> Result<()> {
        if v < ds::NUM_STR_TBL.len() as u16 {
            self.write(ds::NUM_STR_TBL[v as usize].as_bytes())
        } else {
            self.write(v.to_string().as_bytes())
        }
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
