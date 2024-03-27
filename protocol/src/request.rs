use std::{
    fmt::{self, Debug, Display, Formatter},
    ops::{Deref, DerefMut},
    ptr::NonNull,
    sync::{
        atomic::{AtomicUsize, Ordering::*},
        Arc,
    },
};

use ds::time::Instant;

use crate::{callback::CallbackContext, Command, Error, HashedCommand};

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
    #[inline]
    pub fn reset(&self) {
        self.used_us.store(0, Relaxed);
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
    //fn retry_on_rsp_notok(&mut self, retry: bool);
    // 初始化quota
    fn quota(&mut self, quota: BackendQuota);
}

pub struct ContextPtr {
    ctx: NonNull<CallbackContext>,
}

impl crate::Request for ContextPtr {
    #[inline]
    fn start_at(&self) -> ds::time::Instant {
        self.ctx().start_at()
    }
    #[inline]
    fn on_noforward(&mut self) {
        self.ctx().on_noforward();
    }
    #[inline]
    fn on_sent(self) -> Option<Self> {
        if self.ctx().on_sent() {
            Some(self)
        } else {
            None
        }
    }
    #[inline]
    fn on_complete(self, resp: Command) {
        self.ctx().on_complete(resp);
    }
    #[inline]
    fn on_err(self, err: Error) {
        self.ctx().on_err(err);
    }
    #[inline]
    fn mut_context(&mut self) -> &mut Context {
        &mut self.ctx().flag
    }
    #[inline]
    fn write_back(&mut self, wb: bool) {
        self.ctx().write_back = wb;
    }
    #[inline]
    fn try_next(&mut self, goon: bool) {
        self.ctx().try_next = goon;
    }
    //#[inline]
    //fn retry_on_rsp_notok(&mut self, retry: bool) {
    //    self.ctx().retry_on_rsp_notok = retry;
    //}
    #[inline]
    fn quota(&mut self, quota: BackendQuota) {
        self.ctx().quota(quota);
    }
}
impl ContextPtr {
    #[inline]
    pub fn new(ctx: NonNull<CallbackContext>) -> Self {
        Self { ctx }
    }
    #[inline]
    fn ctx(&self) -> &mut CallbackContext {
        unsafe { &mut *self.ctx.as_ptr() }
    }
}

impl Clone for ContextPtr {
    fn clone(&self) -> Self {
        panic!("request sould never be cloned!");
    }
}
impl Display for ContextPtr {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ctx())
    }
}
impl Debug for ContextPtr {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

unsafe impl Send for ContextPtr {}
unsafe impl Sync for ContextPtr {}

impl Deref for ContextPtr {
    type Target = HashedCommand;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.ctx().request()
    }
}
impl DerefMut for ContextPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ctx().request_mut()
    }
}
