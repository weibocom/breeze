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

    #[inline]
    fn check_and_inited(&mut self, write: bool) -> bool {
        if self.ctx > 0 {
            return true;
        }
        let inited = 0b10 | write as u64;
        self.ctx = inited << 62;
        false
    }

    #[inline]
    fn is_write(&self) -> bool {
        self.ctx & (1 << 62) > 0
    }

    // 低8位存储下一次访问的idx，不存在回写的场景，所以读写共享相同的bit
    #[inline]
    fn take_proc_idx(&mut self) -> u8 {
        let idx = self.ctx as u8;
        self.ctx += 1;
        idx
    }

    // fn index(&self) -> u8 {
    //     self.ctx as u8
    // }
}
