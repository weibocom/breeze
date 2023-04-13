use std::{
    mem::MaybeUninit,
    ptr::{self, NonNull},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering::*},
        Arc,
    },
    time::Duration,
};

use ds::{time::Instant, AtomicWaker};

use crate::{request::Request, Command, Error, HashedCommand};

//const REQ_TRY_MAX_COUNT: u8 = 3;

pub struct Callback {
    cb: Box<dyn Fn(Request)>,
}
impl Callback {
    #[inline]
    pub fn new(cb: Box<dyn Fn(Request)>) -> Self {
        Self { cb }
    }
    #[inline]
    pub fn send(&self, req: Request) {
        log::debug!("request sending:{}", req);
        (self.cb)(req);
    }
}

pub struct CallbackContext {
    pub(crate) flag: crate::Context,
    complete: AtomicBool, // 当前请求是否完成
    inited: AtomicBool,   // response是否已经初始化
    async_done: AtomicBool,
    pub(crate) try_next: bool,   // 请求失败是否需要重试
    pub(crate) write_back: bool, // 请求结束后，是否需要回写。
    first: bool,                 // 当前请求是否是所有子请求的第一个
    last: bool,                  // 当前请求是否是所有子请求的最后一个
    async_mode: bool,            // 是否是异步请求
    tries: AtomicU8,
    request: HashedCommand,
    response: MaybeUninit<Command>,
    start: Instant,      // 请求的开始时间
    last_start: Instant, // 本次资源请求的开始时间(一次请求可能触发多次资源请求)
    waker: *const AtomicWaker,
    callback: CallbackPtr,
}

impl CallbackContext {
    #[inline]
    pub fn new(
        req: HashedCommand,
        waker: &AtomicWaker,
        cb: CallbackPtr,
        first: bool,
        last: bool,
    ) -> Self {
        log::debug!("request prepared:{}", req);
        Self {
            first,
            last,
            flag: crate::Context::default(),
            complete: AtomicBool::new(false),
            inited: AtomicBool::new(false),
            async_done: AtomicBool::new(false),
            async_mode: false,
            try_next: false,
            write_back: false,
            request: req,
            response: MaybeUninit::uninit(),
            callback: cb,
            start: Instant::now(),
            last_start: Instant::now(),
            tries: 0.into(),
            waker,
        }
    }

    #[inline]
    pub(crate) fn on_noforward(&mut self) {
        assert!(self.request().noforward(), "{:?}", self);

        // 对noforward请求，只需要设置complete状态为true，不需要wake及其他逻辑
        // self.on_done();
        assert!(!*self.complete.get_mut(), "{:?}", self);
        self.complete.store(true, Release);
    }

    // 返回true: 表示发送完之后还未结束
    // false: 表示请求已结束
    #[inline]
    pub(crate) fn on_sent(&mut self) -> bool {
        log::debug!("request sent: {} ", self);
        if self.request().sentonly() {
            self.on_done();
            false
        } else {
            true
        }
    }
    #[inline]
    pub fn on_complete(&mut self, resp: Command) {
        log::debug!("on-complete:{} resp:{}", self, resp);
        // 异步请求不关注response。
        if !self.async_mode {
            debug_assert!(!self.complete(), "{:?}", self);
            self.swap_response(resp);
        }
        self.on_done();
    }

    #[inline]
    pub fn take_response(&mut self) -> Option<Command> {
        match self.inited.compare_exchange(true, false, AcqRel, Acquire) {
            Ok(_) => unsafe { Some(ptr::read(self.response.as_mut_ptr())) },
            Err(_) => {
                self.write_back = false;
                //assert!(!self.ctx.try_next && !self.ctx.write_back, "{}", self);
                None
            }
        }
    }

    // 只有在构建了response，该request才可以设置completed为true
    #[inline]
    fn on_done(&mut self) {
        log::debug!("on-done:{}", self);
        let goon = if !self.async_mode {
            // 正常访问请求。
            // old: 除非出现了error，否则最多只尝试一次;
            !self.response_ok() && self.try_next && self.tries.fetch_add(1, Release) < 1
        } else {
            // write back请求
            self.write_back
        };

        if goon {
            // 需要重试或回写
            self.last_start = Instant::now();
            return self.goon();
        }
        if !self.async_mode {
            // 说明有请求在pending
            debug_assert!(!self.complete(), "{:?}", self);
            self.complete.store(true, Release);
            unsafe { (&*self.waker).wake() }
        } else {
            self.async_done.store(true, Release);
            // async_mode需要手动释放
            //self.manual_drop();
            //self.ctx
            //    .async_done
            //    .compare_exchange(false, true, AcqRel, Acquire)
            //    .expect("double free?");
        }
    }

    //#[inline]
    //fn need_goon(&self) -> bool {
    //    if !self.ctx.async_mode {
    //        if self.response_ok() || !self.ctx.try_next {
    //            return false;
    //        }
    //        // 正常访问请求。
    //        // old: 除非出现了error，否则最多只尝试一次;
    //        self.tries.fetch_add(1, Release) < 1
    //    } else {
    //        // write back请求
    //        self.ctx.write_back
    //    }
    //}

    #[inline]
    pub fn async_done(&self) -> bool {
        assert!(self.async_mode, "{:?}", self);
        self.async_done.load(Acquire)
    }

    #[inline]
    fn response_ok(&self) -> bool {
        unsafe { self.inited() && self.unchecked_response().ok() }
    }
    #[inline]
    pub fn on_err(&mut self, err: Error) {
        // 正常err场景，仅仅在debug时check
        log::debug!("+++ on_err: {:?} => {:?}", err, self);
        use Error::*;
        match err {
            Closed | ChanDisabled | Waiting | Pending => {}
            _err => log::warn!("on-err:{} {:?}", self, _err),
        }
        self.on_done();
    }
    #[inline]
    pub fn request(&self) -> &HashedCommand {
        &self.request
    }
    #[inline]
    pub fn request_mut(&mut self) -> &mut HashedCommand {
        &mut self.request
    }
    // 在使用前，先得判断inited
    #[inline]
    unsafe fn unchecked_response(&self) -> &Command {
        self.response.assume_init_ref()
    }
    #[inline]
    pub fn complete(&self) -> bool {
        self.complete.load(Acquire)
    }
    #[inline]
    pub fn inited(&self) -> bool {
        self.inited.load(Acquire)
    }
    #[inline]
    pub fn is_write_back(&self) -> bool {
        self.write_back
    }
    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut Self {
        self as *mut _
    }
    #[inline]
    pub fn send(&mut self) {
        let req = Request::new(unsafe { NonNull::new_unchecked(self.as_mut_ptr()) });
        (*self.callback).send(req);
    }
    #[inline]
    pub fn start_at(&self) -> Instant {
        self.start
    }

    // 计算当前请求消耗的时间，更新
    #[inline(always)]
    pub fn elapsed_current_req(&self) -> Duration {
        self.last_start.elapsed()
    }
    #[inline(always)]
    pub fn last_start(&self) -> Instant {
        self.last_start
    }

    #[inline]
    fn goon(&mut self) {
        self.send();
    }
    #[inline]
    pub fn async_mode(&mut self) {
        self.async_mode = true;
    }
    #[inline]
    pub fn with_request(&mut self, req: HashedCommand) {
        assert!(self.async_mode, "{:?}", self);
        self.request = req;
    }
    //#[inline]
    //pub fn as_mut_context(&mut self) -> &mut Context {
    //    &mut self.ctx
    //}
    #[inline]
    fn swap_response(&mut self, resp: Command) {
        if self.inited() {
            log::debug!("drop response:{}", unsafe { self.unchecked_response() });
            //self.ctx
            //    .inited
            //    .compare_exchange(true, false, AcqRel, Relaxed)
            //    .expect("cas failed");
            unsafe { std::ptr::replace(self.response.as_mut_ptr(), resp) };
        } else {
            self.response.write(resp);
            self.inited.store(true, Release);
        }
    }
    #[inline]
    pub fn first(&self) -> bool {
        self.first
    }
    #[inline]
    pub fn last(&self) -> bool {
        self.last
    }
}

impl Drop for CallbackContext {
    #[inline]
    fn drop(&mut self) {
        assert!(*self.complete.get_mut(), "{}", self);
        //self.try_drop_response();
        assert!(!*self.inited.get_mut(), "response not taken:{:?}", self);
        // 可以尝试检查double free
        *self.complete.get_mut() = false;
    }
}

unsafe impl Send for CallbackContext {}
unsafe impl Sync for CallbackContext {}
//#[derive(Default)]
//pub struct Context {}
//
//impl Context {
//    #[inline]
//    pub fn as_mut_flag(&mut self) -> &mut crate::Context {
//        &mut self.flag
//    }
//    #[inline]
//    pub fn try_next(&mut self, goon: bool) {
//        self.try_next = goon;
//    }
//    #[inline]
//    pub fn write_back(&mut self, wb: bool) {
//        self.write_back = wb;
//    }
//    //#[inline]
//    //pub fn is_write_back(&self) -> bool {
//    //    self.write_back
//    //}
//    #[inline]
//    pub fn is_inited(&self) -> bool {
//        self.inited.load(Acquire)
//    }
//}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for CallbackContext {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "complete:{} init:{} async:{} try:{} write back:{} flag:{} tries:{} => {:?}",
            self.complete.load(Acquire),
            self.inited(),
            self.async_mode,
            self.try_next,
            self.write_back,
            self.flag,
            self.tries.load(Acquire),
            self.request,
        )
    }
}
impl Debug for CallbackContext {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

//impl Display for Context {
//    #[inline]
//    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//        write!(
//            f,
//            "complete:{} init:{} async:{} try:{} write back:{} flag:{}",
//            self.complete.load(Acquire),
//            self.is_inited(),
//            self.async_mode,
//            self.try_next,
//            self.write_back,
//            self.flag,
//        )
//    }
//}

unsafe impl Send for CallbackPtr {}
unsafe impl Sync for CallbackPtr {}
unsafe impl Send for Callback {}
unsafe impl Sync for Callback {}
#[derive(Clone)]
pub struct CallbackPtr {
    ptr: Arc<Callback>,
}
impl std::ops::Deref for CallbackPtr {
    type Target = Callback;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.ptr.as_ref()
    }
}
impl From<Callback> for CallbackPtr {
    // 调用方确保CallbackPtr在使用前，指针的有效性。
    fn from(cb: Callback) -> Self {
        Self { ptr: Arc::new(cb) }
    }
}
