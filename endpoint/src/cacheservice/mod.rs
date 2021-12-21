mod config;
pub mod topo;

// 63位用来标识是否初始化了。
// 62次高位存储请求类型：0是get, 1是set
// 0~48位：是索引。
#[repr(transparent)]
struct Context {
    ctx: protocol::Context,
}

const H_MASK: u64 = 0xffff << 48;
impl Context {
    #[inline(always)]
    fn from(ctx: protocol::Context) -> Self {
        Self { ctx }
    }
    // 检查是否初始化，如果未初始化则进行初始化。
    #[inline(always)]
    fn check_and_inited(&mut self, write: bool) -> bool {
        if self.ctx > 0 {
            true
        } else {
            let inited = 0b10 | write as u64;
            self.ctx = inited << 62;
            false
        }
    }
    #[inline(always)]
    fn is_write(&self) -> bool {
        self.ctx & (1 << 62) > 0
    }
    // 获取idx，并将原有的idx+1
    #[inline(always)]
    fn take_write_idx(&mut self) -> u16 {
        let idx = self.ctx as u16;
        self.ctx += 1;
        idx
    }
    // 低16位存储是下一次的idx
    // 如果是写请求，低16位，是索引
    // 如果是读请求，则
    #[inline(always)]
    fn take_read_idx(&mut self) -> u16 {
        let mut low_48bit = self.low();
        let hight_16bit = self.hight();

        // 低16位是最后一次读取的idx。需要取16~31位的值
        low_48bit >>= 16;
        let idx = low_48bit as u16;

        self.ctx = hight_16bit | low_48bit;
        idx
    }
    #[inline(always)]
    fn hight(&self) -> u64 {
        self.ctx & H_MASK
    }
    #[inline(always)]
    fn low(&self) -> u64 {
        self.ctx & (!H_MASK)
    }
    // 把idx写入到低48位。原有的idx往高位移动。
    #[inline(always)]
    fn write_back_idx(&mut self, idx: u16) {
        let hight_16bit = self.hight();
        let low_48bit = (self.low() << 16) | idx as u64;
        self.ctx = hight_16bit | low_48bit;
    }
    #[inline(always)]
    fn index(&self) -> u16 {
        self.ctx as u16
    }
}
