pub(super) mod config;
pub mod topo;

pub use config::*;

// 最高位63位标识是否初始化了；
// 62次高位存储请求类型：0是get，1是set；
// 最低8bit做读写索引
#[repr(transparent)]
pub struct Context {
    ctx: protocol::Context,
}

impl Context {
    #[inline]
    fn from(ctx: protocol::Context) -> Self {
        Self { ctx }
    }
    //#[inline]
    //fn inited(&self) -> bool {
    //    self.ctx & (1 << 63) != 0
    //}
    //#[inline]
    //fn check_inited(&mut self) {
    //    self.ctx = self.ctx | 1 << 63;
    //}

    //#[inline]
    //fn check_and_inited(&mut self, write: bool) -> bool {
    //    if self.ctx > 0 {
    //        return true;
    //    }
    //    let inited = 0b10 | write as u64;
    //    self.ctx = inited << 62;
    //    false
    //}

    //#[inline]
    //fn is_write(&self) -> bool {
    //    self.ctx & (1 << 62) > 0
    //}

    //// 低8位存储下一次访问的idx，phantom的回写实际是多写，所以读写共享相同的bit即可
    //#[inline]
    //fn take_proc_idx(&mut self) -> u8 {
    //    let idx = self.ctx as u8;
    //    self.ctx += 1;
    //    idx
    //}

    #[inline]
    fn index(&self) -> usize {
        self.ctx as u8 as usize
    }
    #[inline]
    fn fetch_add_idx(&mut self) -> usize {
        let old = self.ctx as u8 as usize;
        self.ctx += 1;
        old
    }
    //#[inline]
    //fn update_idx(&mut self, idx: usize) {
    //    self.ctx = idx as u64;
    //}
}
