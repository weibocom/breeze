use std::{
    mem::MaybeUninit,
    ptr::{self, NonNull},
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering::*},
        Arc,
    },
};

use crate::{Attachment, BackendQuota};
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
    pub(crate) max_tries: u8,            // 最大重试次数
    first: bool,                         // 当前请求是否是所有子请求的第一个
    last: bool,                          // 当前请求是否是所有子请求的最后一个
    tries: AtomicU8,
    request: HashedCommand,
    response: MaybeUninit<Command>,
    start: Instant, // 请求的开始时间
    waker: *const Arc<AtomicWaker>,
    callback: CallbackPtr,
    quota: Option<BackendQuota>,
    attachment: Option<Attachment>, // 附加数据，用于辅助请求和响应，目前只有kvector在使用
    drop_attach: Option<Box<dyn Fn(Attachment)>>,
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
        max_tries: u8,
        drop_attach: Option<Box<dyn Fn(Attachment)>>,
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
            max_tries,
            request: req,
            response: MaybeUninit::uninit(),
            callback: cb,
            start: now,
            tries: 0.into(),
            waker,
            quota: None,
            attachment: None,
            drop_attach,
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
    pub fn on_complete<P: crate::Proto>(&mut self, parser: &P, resp: Command) {
        log::debug!("on-complete:{} resp:{}", self, resp);
        // 异步请求不关注response。
        if !self.async_mode {
            debug_assert!(!self.complete(), "{:?}", self);
            if self.attachment.is_some() {
                // vector聚合场景
                self.on_complete_aggregate(parser, resp);
            } else {
                self.swap_response(resp);
            }
        }
        self.on_done();
    }

    #[inline]
    pub fn on_complete_aggregate<P: crate::Proto>(&mut self, parser: &P, mut resp: Command) {
        // 返回成功：
        //   1. 第一轮获取si；若si获取失败（例如si为空），则终止请求
        //   2. 后续轮次更新attachment，并判断是否是最后一轮。
        // 返回失败，则终止请求。
        if resp.ok() {
            // 更新attachment
            let attach = self.attachment.as_mut().expect("attach");
            let last = parser.update_attachment(attach, &mut resp);
            if last {
                self.set_last(true);
            }
            // 更新attachment不成功，或者响应数足够,终止请求
        } else {
            self.set_last(true);
        }
        if self.last() {
            // 中间轮次的resp没有被使用，可忽略;
            self.swap_response(resp);
        } else {
            // 重置下一轮访问需要的变量
            self.try_next = true; // 可以进入下一轮访问
            self.set_fitst_try();
        }
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
                // rsp.ok
                if unsafe { self.unchecked_response().ok() } {
                    return false;
                }
                //有响应并且!ok，配置了!retry_on_rsp_notok，不需要重试，比如mysql
                if !self.retry_on_rsp_notok {
                    return false;
                }
            }

            self.try_next && self.tries.fetch_add(1, Release) < self.max_tries
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

        // 改到这里，不需要额外判断逻辑了
        self.set_last(true);

        //markdone后，req标记为已完成，那么CallbackContext和CopyBidirectional都有可能被释放
        //CopyBidirectional会提前释放，所以需要提前clone一份
        //CallbackContext会提前释放，则需要在此clone到栈上
        //async_mode同理
        let waker = unsafe { self.waker.as_ref().unwrap().clone() };
        let async_mode = self.async_mode;
        self.mark_done();
        //！！！mark_done之后禁止使用self，self有可能已被释放
        if !async_mode {
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
            ChanDisabled | Waiting | Pending => {}
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
    #[inline]
    pub fn attachment(&self) -> Option<&Attachment> {
        self.attachment.as_ref()
    }
    #[inline]
    pub fn attachment_mut(&mut self) -> &mut Option<Attachment> {
        &mut self.attachment
    }
    #[inline]
    pub fn set_last(&mut self, last: bool) {
        // todo: 可优化为依据请求数或者响应数量判断可以设置last为true
        self.last = last;
    }
    #[inline]
    pub fn set_max_tries(&mut self, max_tries: u8) {
        self.max_tries = max_tries;
    }
    #[inline]
    pub fn set_fitst_try(&mut self) {
        self.tries = 0.into();
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
        if let Some(attachment) = self.attachment.take() {
            (self.drop_attach.as_ref().expect("should has drop_attach"))(attachment);
        }
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
