mod config;
pub mod topo;

// 63位用来标识是否初始化了。
// 62次高位存储请求类型：0是get, 1是set
// 0~48位：是索引。
#[repr(transparent)]
struct Context {
    ctx: protocol::Context,
}

// 高16位的mask，供store、retrive两种cmds共享
const H_MASK: u64 = 0xffff << 48;
// store cmds的skip slave的bit位
const STORE_SKIP_SLAVE: u64 = 0x1 << 16;

impl Context {
    #[inline]
    fn from(ctx: protocol::Context) -> Self {
        Self { ctx }
    }
    // 检查是否初始化，如果未初始化则进行初始化。
    #[inline]
    fn check_and_inited(&mut self, write: bool) -> bool {
        if self.ctx > 0 {
            true
        } else {
            let inited = 0b10 | write as u64;
            self.ctx = inited << 62;
            false
        }
    }
    #[inline]
    fn is_write(&self) -> bool {
        self.ctx & (1 << 62) > 0
    }
    // 获取write操作的idx，并将原有的idx+1
    #[inline]
    fn take_write_idx(&mut self) -> u16 {
        let idx = self.ctx as u16;
        self.ctx += 1;
        idx
    }

    /// 对store类型，doublebase时，对于cas/casq/add/addq操作slave时，需要设置该bit，后面回写时，需要skip slave_idx
    /// 本操作仅对store类型cmds生效
    #[inline(always)]
    fn set_skip_slave_4store(&mut self) {
        assert!(self.is_write());
        self.ctx |= STORE_SKIP_SLAVE;
    }

    /// 对store类型，回写时是否需要skip slave
    #[inline(always)]
    fn need_skip_slave_4store(&self) -> bool {
        self.ctx & STORE_SKIP_SLAVE > 0
    }

    // 如果是写请求，低16位是写索引，第17个bit存放是否回写slave
    // 如果是读请求，则最低16bits存访当前idx，访问下一层时，将前一次的idx向高位左移16bits
    #[inline]
    fn take_read_idx(&mut self) -> u16 {
        let mut low_48bit = self.low();
        let hight_16bit = self.hight();

        // 低16位是最后一次读取的idx。需要取16~31位的值
        low_48bit >>= 16;
        let idx = low_48bit as u16;

        self.ctx = hight_16bit | low_48bit;
        idx
    }
    #[inline]
    fn hight(&self) -> u64 {
        self.ctx & H_MASK
    }
    #[inline]
    fn low(&self) -> u64 {
        self.ctx & (!H_MASK)
    }
    // 把idx写入到低48位。原有的idx往高位移动。
    #[inline]
    fn write_back_idx(&mut self, idx: u16) {
        let hight_16bit = self.hight();
        let low_48bit = (self.low() << 16) | idx as u64;
        self.ctx = hight_16bit | low_48bit;
    }
    #[inline]
    fn index(&self) -> u16 {
        self.ctx as u16
    }
    #[inline]
    fn inited(&self) -> bool {
        self.ctx != 0
    }
}
