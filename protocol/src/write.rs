use super::Result;
const NUM_STR_TBL: [&'static str; 32] = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
    "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
];
pub trait Writer {
    fn pending(&self) -> usize;
    // 写数据，一次写完
    fn write(&mut self, data: &[u8]) -> Result<()>;
    #[inline]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.write(&[v])
    }
    // 按str类型写入数字
    // write(v.to_string().as_bytes())
    #[inline]
    fn write_s_u16(&mut self, v: u16) -> Result<()> {
        if v < NUM_STR_TBL.len() as u16 {
            self.write(NUM_STR_TBL[v as usize].as_bytes())
        } else {
            self.write(v.to_string().as_bytes())
        }
    }

    // hint: 提示可能优先写入到cache
    #[inline]
    fn cache(&mut self, _hint: bool) {}

    #[inline]
    fn write_slice(&mut self, data: &ds::RingSlice, oft: usize) -> Result<()> {
        let mut oft = oft;
        let len = data.len();
        log::debug!("+++ will write to client/server:{:?}", data);
        while oft < len {
            let data = data.read(oft);
            oft += data.len();
            if oft < len {
                // 说明有多次写入，将其cache下来
                self.cache(true);
            }
            self.write(data)?;
        }
        Ok(())
    }
}
impl Writer for Vec<u8> {
    #[inline]
    fn pending(&self) -> usize {
        self.len()
    }
    #[inline]
    fn write(&mut self, data: &[u8]) -> Result<()> {
        ds::vec::Buffer::write(self, data);
        Ok(())
    }
    #[inline]
    fn write_u8(&mut self, v: u8) -> Result<()> {
        self.push(v);
        Ok(())
    }
}
