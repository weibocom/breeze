use std::{
    mem::MaybeUninit,
    ptr::{self, NonNull},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering::*},
        Arc,
    },
};

use crate::BackendQuota;
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
    async_mode: bool,                    // 是否是异步请求
    done: AtomicBool,                    // 当前模式请求是否完成
    inited: AtomicBool,                  // response是否已经初始化
    pub(crate) try_next: bool,           // 请求失败后，topo层面是否允许重试
    pub(crate) retry_on_rsp_notok: bool, // 有响应且响应不ok时，协议层面是否允许重试
    pub(crate) write_back: bool,         // 请求结束后，是否需要回写。
    first: bool,                         // 当前请求是否是所有子请求的第一个
    last: bool,                          // 当前请求是否是所有子请求的最后一个
    tries: AtomicU8,
    request: HashedCommand,
    response: MaybeUninit<Command>,
    start: Instant, // 请求的开始时间
    waker: *const Arc<AtomicWaker>,
    callback: CallbackPtr,
    quota: Option<BackendQuota>,
}

impl CallbackContext {
    #[inline]
    pub fn new(
        req: HashedCommand,
        waker: *const Arc<AtomicWaker>,
        cb: CallbackPtr,
        first: bool,
        last: bool,
        retry_on_rsp_notok: bool,
    ) -> Self {
        log::debug!("request prepared:{}", req);
        let now = Instant::now();
        Self {
            first,
            last,
            flag: crate::Context::default(),
            done: AtomicBool::new(false),
            inited: AtomicBool::new(false),
            async_mode: false,
            try_next: false,
            retry_on_rsp_notok,
            write_back: false,
            request: req,
            response: MaybeUninit::uninit(),
            callback: cb,
            start: now,
            tries: 0.into(),
            waker,
            quota: None,
        }
    }

    #[inline]
    pub fn flag(&self) -> crate::Context {
        self.flag
    }

    #[inline]
    pub(crate) fn on_noforward(&mut self) {
        debug_assert!(self.request().noforward(), "{:?}", self);
        self.mark_done();
    }
    // 在请求结束之后，设置done为true
    #[inline(always)]
    fn mark_done(&self) {
        debug_assert!(!self.done.load(Acquire), "{:?}", self);
        self.done.store(true, Release);
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

    #[inline]
    fn need_gone(&self) -> bool {
        if !self.async_mode {
            // 当前重试条件为 rsp == None || ("mc" && !rsp.ok())
            if self.inited() {
                // 优先筛出正常的请求，便于理解
                // rsp.ok 不需要重试
                if unsafe { self.unchecked_response().ok() } {
                    return false;
                }
                //有响应并且!ok，配置了!retry_on_rsp_notok，不需要重试，比如mysql
                if !self.retry_on_rsp_notok {
                    return false;
                }
            }
            self.try_next && self.tries.fetch_add(1, Release) < 1
        } else {
            // write back请求
            self.write_back
        }
    }

    // 只有在构建了response，该request才可以设置completed为true
    #[inline]
    fn on_done(&mut self) {
        log::debug!("on-done:{}", self);
        if !self.async_mode {
            // 更新backend使用的时间
            self.quota.take().map(|q| q.incr(self.start_at().elapsed()));
        }

        if self.need_gone() {
            // 需要重试或回写
            return self.goon();
        }
        //防止markdone后，在pipeline中req被释放，req和waker被覆写
        let waker = unsafe { self.waker.as_ref().unwrap().clone() };
        self.mark_done();
        if !self.async_mode {
            waker.wake()
        }
    }

    #[inline]
    pub fn async_done(&self) -> bool {
        debug_assert!(self.async_mode, "{:?}", self);
        self.done.load(Acquire)
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
        // 一次错误至少消耗500ms的配额
        self.quota
            .take()
            .map(|q| q.err_incr(self.start_at().elapsed()));
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
        debug_assert!(!self.async_mode, "{:?}", self);
        self.done.load(Acquire)
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

    #[inline]
    fn goon(&mut self) {
        self.send();
    }
    #[inline]
    pub fn async_mode(&mut self) {
        // 在异步处理之前，必须要先处理完response
        debug_assert!(
            !self.inited() && self.complete() && !self.async_mode,
            "{:?}",
            self
        );
        self.async_mode = true;
        self.done
            .compare_exchange(true, false, AcqRel, Relaxed)
            .expect("sync mode not done");
    }
    #[inline]
    pub fn with_request(&mut self, req: HashedCommand) {
        debug_assert!(self.async_mode, "{:?}", self);
        self.request = req;
    }
    #[inline]
    fn swap_response(&mut self, resp: Command) {
        if self.inited() {
            log::debug!("drop response:{}", unsafe { self.unchecked_response() });
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
    #[inline]
    pub fn quota(&mut self, quota: BackendQuota) {
        self.quota = Some(quota);
    }
}

impl Drop for CallbackContext {
    #[inline]
    fn drop(&mut self) {
        debug_assert!(*self.done.get_mut(), "{}", self);
        debug_assert!(!*self.inited.get_mut(), "response not taken:{:?}", self);
        // 可以尝试检查double free
        // 在debug环境中，设置done为false
        debug_assert_eq!(*self.done.get_mut() = false, ());
    }
}

use std::fmt::{self, Debug, Display, Formatter};
impl Display for CallbackContext {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "async mod:{} done:{} init:{} try_next:{} retry_on_notok:{} write back:{} flag:{} tries:{} => {:?}",
            self.async_mode,
            self.done.load(Acquire),
            self.inited(),
            self.try_next,
            self.retry_on_rsp_notok,
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
