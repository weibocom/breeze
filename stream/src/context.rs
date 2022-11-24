use std::{marker::PhantomData, ops::AddAssign, ptr::NonNull, sync::Arc};

use protocol::{
    callback::CallbackContext, request::Request, Command, Commander, HashedCommand, Metric,
    MetricName, Protocol,
};

use ds::arena::Arena;

pub(super) struct CallbackContextPtr {
    ptr: NonNull<CallbackContext>,
    arena: NonNull<Arena<CallbackContext>>,
}

impl CallbackContextPtr {
    #[inline]
    pub fn build_request(&mut self) -> Request {
        Request::new(self.ptr)
    }
    //需要在on_done时主动销毁self对象
    #[inline]
    pub(super) fn async_write_back<P: Protocol, M: Metric<T>, T: std::ops::AddAssign<i64>>(
        &mut self,
        parser: &P,
        mut res: Command,
        exp: u32,
        metric: &mut Arc<M>,
    ) {
        // 在异步处理之前，必须要先处理完response
        assert!(!self.inited() && self.complete(), "cbptr:{:?}", &**self);
        self.async_mode();
        if let Some(new) = parser.build_writeback_request(
            &mut ResponseContext::new(self, Some(&mut res), metric, |_h| 0),
            exp,
        ) {
            self.with_request(new);
        }
        log::debug!("start write back:{}", &**self);

        self.send();
    }
}

impl CallbackContextPtr {
    #[inline]
    pub(crate) fn from(ptr: NonNull<CallbackContext>, arena: &mut Arena<CallbackContext>) -> Self {
        let arena = unsafe { NonNull::new_unchecked(arena) };
        Self { ptr, arena }
    }
}
impl Drop for CallbackContextPtr {
    #[inline]
    fn drop(&mut self) {
        // CallbackContextPtr 在释放时，对arena持有一个mut 引用，因此是safe的。
        // 另外，在CopyBidirectional中使用时，会确保所有的CallbackContextPtr对象释放后，才会销毁arena
        let arena = unsafe { &mut *self.arena.as_ptr() };
        arena.dealloc(self.ptr);
    }
}

impl std::ops::Deref for CallbackContextPtr {
    type Target = CallbackContext;
    #[inline]
    fn deref(&self) -> &Self::Target {
        unsafe { self.ptr.as_ref() }
    }
}
impl std::ops::DerefMut for CallbackContextPtr {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.ptr.as_ptr() }
    }
}
unsafe impl Send for CallbackContextPtr {}
unsafe impl Sync for CallbackContextPtr {}

pub struct ResponseContext<'a, M: Metric<T>, T: std::ops::AddAssign<i64>, F: Fn(i64) -> usize> {
    // ctx 中的response不可直接用，先封住，按需暴露
    ctx: &'a mut CallbackContextPtr,
    pub response: Option<&'a mut Command>,
    pub metrics: &'a mut Arc<M>,
    dist_fn: F,
    _mark: PhantomData<T>,
}

impl<'a, M: Metric<T>, T: AddAssign<i64>, F: Fn(i64) -> usize> ResponseContext<'a, M, T, F> {
    pub(super) fn new(
        ctx: &'a mut CallbackContextPtr,
        response: Option<&'a mut Command>,
        metrics: &'a mut Arc<M>,
        dist_fn: F,
    ) -> Self {
        Self {
            ctx,
            response,
            metrics,
            dist_fn,
            _mark: Default::default(),
        }
    }
}

impl<'a, M: Metric<T>, T: AddAssign<i64>, F: Fn(i64) -> usize> Commander
    for ResponseContext<'a, M, T, F>
{
    #[inline]
    fn request_mut(&mut self) -> &mut HashedCommand {
        self.ctx.request_mut()
    }
    #[inline]
    fn request(&self) -> &HashedCommand {
        self.ctx.request()
    }
    #[inline]
    fn request_shard(&self) -> usize {
        (self.dist_fn)(self.request().hash())
    }
    #[inline]
    fn response(&self) -> Option<&Command> {
        if let Some(rsp) = &self.response {
            Some(rsp)
        } else {
            None
        }
    }
    #[inline]
    fn response_mut(&mut self) -> Option<&mut Command> {
        if let Some(rsp) = &mut self.response {
            Some(rsp)
        } else {
            None
        }
    }
}

impl<'a, M: Metric<T>, T: std::ops::AddAssign<i64>, F: Fn(i64) -> usize> Metric<T>
    for ResponseContext<'a, M, T, F>
{
    #[inline]
    fn get(&self, name: MetricName) -> &mut T {
        self.metrics.get(name)
    }
}
