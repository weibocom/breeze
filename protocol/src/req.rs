use ds::time::Instant;
use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering::*};
use std::sync::Arc;

use crate::{Command, HashedCommand};

pub type Context = u64;

#[repr(transparent)]
#[derive(Clone, Default)]
pub struct BackendQuota {
    used_us: Arc<AtomicUsize>, // 所有副本累计使用的时间
}
impl BackendQuota {
    #[inline]
    pub fn incr(&self, d: ds::time::Duration) {
        self.used_us.fetch_add(d.as_micros() as usize, Relaxed);
    }
    #[inline]
    pub fn err_incr(&self, d: ds::time::Duration) {
        // 一次错误请求，消耗500ms
        self.used_us
            .fetch_add(d.as_micros().max(500_000) as usize, Relaxed);
    }
    // 配置时间的微秒计数
    #[inline]
    pub fn us(&self) -> usize {
        self.used_us.load(Relaxed)
    }

    pub fn set_used_us(&self, used_us: usize) {
        self.used_us.store(used_us, Relaxed);
    }
}

pub trait Request:
    Debug
    + Display
    + Send
    + Sync
    + 'static
    + Unpin
    + Sized
    + Deref<Target = HashedCommand>
    + DerefMut<Target = HashedCommand>
{
    fn start_at(&self) -> Instant;
    fn on_noforward(&mut self);
    fn on_sent(self) -> Option<Self>;
    fn on_complete(self, resp: Command);
    fn on_err(self, err: crate::Error);
    #[inline]
    fn context_mut(&mut self) -> &mut Context {
        self.mut_context()
    }
    fn mut_context(&mut self) -> &mut Context;
    // 请求成功后，是否需要进行回写或者同步。
    fn write_back(&mut self, wb: bool);
    //fn is_write_back(&self) -> bool;
    // 请求失败后，topo层面是否允许进行重试
    fn try_next(&mut self, goon: bool);
    // 请求失败后，协议层面是否允许进行重试
    fn retry_on_rsp_notok(&mut self, retry: bool);
    // 初始化quota
    fn quota(&mut self, quota: BackendQuota);
}
