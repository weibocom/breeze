use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use ds::AtomicWaker;

use crate::request::Request;
use crate::{Command, Error, HashedCommand};

pub struct Callback {
    exp_sec: u32,
    receiver: usize,
    cb: fn(usize, Request),
}
impl Callback {
    #[inline]
    pub fn new(receiver: usize, cb: fn(usize, Request)) -> Self {
        let exp_sec = 86400;
        Self {
            receiver,
            cb,
            exp_sec,
        }
    }
    #[inline(always)]
    pub fn send(&self, req: Request) {
        log::debug!("request sending:{}", req);
        (self.cb)(self.receiver, req);
    }
    #[inline(always)]
    pub fn exp_sec(&self) -> u32 {
        self.exp_sec
    }
}

pub struct CallbackContext {
    pub(crate) ctx: Context,
    request: HashedCommand,
    response: MaybeUninit<Command>,
    waker: *const AtomicWaker,
    callback: CallbackPtr,
    start: Instant,
}

impl CallbackContext {
    #[inline(always)]
    pub fn new(
        req: HashedCommand,
        waker: &AtomicWaker,
        cb: CallbackPtr,
        first: bool,
        last: bool,
    ) -> Self {
        let mut ctx = Context::default();
        ctx.first = first;
        ctx.last = last;
        log::debug!("request prepared:{}", req);
        Self {
            ctx,
            waker: waker as *const _,
            request: req,
            response: MaybeUninit::uninit(),
            callback: cb,
            start: Instant::now(),
        }
    }

    #[inline(always)]
    pub fn on_sent(&mut self) {
        log::debug!("request sent: {} ", self);
        if self.request().sentonly() {
            self.on_done();
        }
    }
    #[inline(always)]
    pub fn on_complete(&mut self, resp: Command) {
        log::debug!("on-complete:{} resp:{}", self, resp);
        // 异步请求不关注response。
        if !self.is_in_async_write_back() {
            self.write(resp);
        }
        self.on_done();
    }
    #[inline(always)]
    pub fn on_response(&self) {}
    #[inline(always)]
    fn on_done(&mut self) {
        log::debug!("on-done:{}", self);
        if self.need_goon() {
            return self.continute();
        }
        if !self.ctx.drop_on_done {
            // 说明有请求在pending
            debug_assert!(!self.complete());
            self.ctx.complete.store(true, Ordering::Release);
            self.wake();
        } else {
            self.manual_drop();
        }
    }
    #[inline(always)]
    fn need_goon(&self) -> bool {
        if !self.is_in_async_write_back() {
            // 正常访问请求。
            self.ctx.try_next && !self.response_ok()
        } else {
            // write back请求
            self.ctx.write_back
        }
    }
    #[inline(always)]
    fn response_ok(&self) -> bool {
        unsafe { self.ctx.inited && self.response().ok() }
    }
    #[inline(always)]
    pub fn on_err(&mut self, err: Error) {
        match err {
            Error::Closed => {}
            err => log::info!("on-err:{} {}", self, err),
        }
        self.on_done();
    }
    #[inline(always)]
    pub fn request(&self) -> &HashedCommand {
        &self.request
    }
    #[inline(always)]
    pub fn with_request(&mut self, new: HashedCommand) {
        self.request = new;
    }
    // 在使用前，先得判断inited
    #[inline(always)]
    pub unsafe fn response(&self) -> &Command {
        debug_assert!(self.inited());
        self.response.assume_init_ref()
    }
    #[inline(always)]
    pub fn complete(&self) -> bool {
        self.ctx.complete.load(Ordering::Acquire)
    }
    #[inline(always)]
    pub fn inited(&self) -> bool {
        self.ctx.inited
    }
    #[inline(always)]
    fn is_write_back(&self) -> bool {
        self.ctx.write_back
    }
    #[inline(always)]
    fn write(&mut self, resp: Command) {
        debug_assert!(!self.complete());
        self.try_drop_response();
        self.response.write(resp);
        self.ctx.inited = true;
    }
    #[inline(always)]
    fn wake(&self) {
        unsafe { (&*self.waker).wake() }
    }
    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut Self {
        self as *mut _
    }
    #[inline(always)]
    pub fn start(&mut self) {
        log::debug!("request started:{}", self);
        if self.request().noforward() {
            // 不需要转发，直接结束。response没有初始化，在write_response里面处理。
            self.on_done();
        } else {
            self.send();
        }
    }
    #[inline(always)]
    pub fn send(&mut self) {
        let req = Request::new(self.as_mut_ptr());
        (*self.callback).send(req);
    }
    #[inline(always)]
    pub fn start_at(&self) -> Instant {
        self.start
    }

    #[inline(always)]
    fn continute(&mut self) {
        self.send();
    }
    #[inline(always)]
    pub fn as_mut_context(&mut self) -> &mut Context {
        &mut self.ctx
    }
    #[inline(always)]
    fn is_in_async_write_back(&self) -> bool {
        self.ctx.drop_on_done
    }
    #[inline(always)]
    fn try_drop_response(&mut self) {
        if self.ctx.inited {
            log::debug!("drop response:{}", unsafe { self.response() });
            unsafe { std::ptr::drop_in_place(self.response.as_mut_ptr()) };
            self.ctx.inited = false;
        }
    }
    #[inline(always)]
    pub fn first(&self) -> bool {
        self.ctx.first
    }
    #[inline(always)]
    pub fn last(&self) -> bool {
        self.ctx.last
    }
    #[inline(always)]
    fn manual_drop(&mut self) {
        unsafe { Box::from_raw(self) };
    }
}

impl Drop for CallbackContext {
    #[inline(always)]
    fn drop(&mut self) {
        debug_assert!(self.complete());
        if self.ctx.inited {
            unsafe {
                std::ptr::drop_in_place(self.response.as_mut_ptr());
            }
        }
    }
}

unsafe impl Send for CallbackContext {}
unsafe impl Sync for CallbackContext {}
#[derive(Default)]
pub struct Context {
    drop_on_done: bool,   // on_done时，是否手工销毁
    try_next: bool,       // 请求失败是否需要重试
    complete: AtomicBool, // 当前请求是否完成
    inited: bool,         // response是否已经初始化
    write_back: bool,     // 请求结束后，是否需要回写。
    first: bool,          // 当前请求是否是所有子请求的第一个
    last: bool,           // 当前请求是否是所有子请求的最后一个
    flag: crate::Context,
}

impl Context {
    #[inline(always)]
    pub fn as_mut_flag(&mut self) -> &mut crate::Context {
        &mut self.flag
    }
    #[inline(always)]
    pub fn try_next(&mut self, goon: bool) {
        self.try_next = goon;
    }
    #[inline(always)]
    pub fn write_back(&mut self, wb: bool) {
        self.write_back = wb;
    }
    #[inline(always)]
    pub fn is_write_back(&self) -> bool {
        self.write_back
    }
    #[inline(always)]
    pub fn is_inited(&self) -> bool {
        self.inited
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for CallbackContext {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.ctx, self.request())
    }
}
impl Debug for CallbackContext {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl Display for Context {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "complete:{} init:{},async mode:{} try_next:{} write back:{}  context:{}",
            self.complete.load(Ordering::Relaxed),
            self.inited,
            self.drop_on_done,
            self.try_next,
            self.write_back,
            self.flag
        )
    }
}

use std::ptr::NonNull;
pub struct CallbackContextPtr {
    inner: NonNull<CallbackContext>,
}

impl CallbackContextPtr {
    #[inline(always)]
    pub fn build_request(&mut self) -> Request {
        unsafe { Request::new(self.inner.as_mut()) }
    }
    //需要在on_done时主动销毁self对象
    #[inline(always)]
    pub fn async_start_write_back<P: crate::Protocol>(mut self, parser: &P, exp: u32) {
        debug_assert!(self.ctx.inited);
        debug_assert!(self.complete());
        if !self.is_write_back() || !unsafe { self.response().ok() } {
            return;
        }
        if let Some(new) = parser.build_writeback_request(&mut self, exp) {
            self.with_request(new);
        }
        // 还会有异步请求，内存释放交给异步操作完成后的on_done来处理
        self.ctx.drop_on_done = true;
        unsafe {
            let ctx = self.inner.as_mut();
            ctx.try_drop_response();
            // write_back请求是异步的，不需要response
            log::debug!("start write back:{}", self.inner.as_ref());
            ctx.continute();
        }
    }
}

impl From<CallbackContext> for CallbackContextPtr {
    #[inline(always)]
    fn from(ctx: CallbackContext) -> Self {
        let ptr = Box::leak(Box::new(ctx));
        let inner = unsafe { NonNull::new_unchecked(ptr) };
        Self { inner }
    }
}

impl Drop for CallbackContextPtr {
    #[inline(always)]
    fn drop(&mut self) {
        // 如果ignore为true，说明当前内存手工释放
        unsafe {
            if !self.inner.as_ref().ctx.drop_on_done {
                self.manual_drop();
            }
        }
    }
}
use std::ops::{Deref, DerefMut};
impl Deref for CallbackContextPtr {
    type Target = CallbackContext;
    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe {
            debug_assert!(!self.inner.as_ref().ctx.drop_on_done);
            self.inner.as_ref()
        }
    }
}
impl DerefMut for CallbackContextPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            debug_assert!(!self.inner.as_ref().ctx.drop_on_done);
            self.inner.as_mut()
        }
    }
}
unsafe impl Send for CallbackContextPtr {}
unsafe impl Sync for CallbackContextPtr {}
unsafe impl Send for CallbackPtr {}
unsafe impl Sync for CallbackPtr {}
#[derive(Clone)]
pub struct CallbackPtr {
    ptr: *const Callback,
}
impl Deref for CallbackPtr {
    type Target = Callback;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        debug_assert!(!self.ptr.is_null());
        unsafe { &*self.ptr }
    }
}
impl From<&Callback> for CallbackPtr {
    // 调用方确保CallbackPtr在使用前，指针的有效性。
    fn from(cb: &Callback) -> Self {
        Self {
            ptr: cb as *const _,
        }
    }
}

impl crate::Commander for CallbackContextPtr {
    #[inline(always)]
    fn request_mut(&mut self) -> &mut HashedCommand {
        &mut self.request
    }
    #[inline(always)]
    fn request(&self) -> &HashedCommand {
        &self.request
    }
    #[inline(always)]
    fn response(&self) -> &Command {
        debug_assert!(self.ctx.inited);
        unsafe { self.inner.as_ref().response() }
    }
    #[inline(always)]
    fn response_mut(&mut self) -> &mut Command {
        debug_assert!(self.inited());
        unsafe { self.response.assume_init_mut() }
    }
}
