use std::{marker::PhantomData, ptr::NonNull, sync::Arc};

use protocol::{
    callback::CallbackContext, request::Request, Command, Commander, HashedCommand, Metric,
    MetricItem, Protocol,
};

use crate::arena::CallbackContextArena;

pub(crate) struct CallbackContextPtr {
    ptr: NonNull<CallbackContext>,
    arena: NonNull<CallbackContextArena>,
}

impl CallbackContextPtr {
    #[inline]
    pub fn build_request(&mut self) -> Request {
        Request::new(self.ptr)
    }
    //需要在on_done时主动销毁self对象
    #[inline]
    pub(super) fn async_write_back<
        P: Protocol,
        M: Metric<T>,
        T: std::ops::AddAssign<i64> + std::ops::AddAssign<bool>,
    >(
        &mut self,
        parser: &P,
        resp: Command,
        exp: u32,
        metric: &mut Arc<M>,
    ) {
        self.async_mode();
        let mut rsp_ctx = ResponseContext::new(self, metric, |_h| {
            assert!(false, "write back"); // 此处的dist_fn逻辑上暂时不会用
            0
        });
        if let Some(new) = parser.build_writeback_request(&mut rsp_ctx, &resp, exp) {
            self.with_request(new);
        }
        log::debug!("start write back:{}", &**self);

        self.send();
    }
}

impl CallbackContextPtr {
    #[inline]
    pub(crate) fn from(ptr: NonNull<CallbackContext>, arena: &mut CallbackContextArena) -> Self {
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

pub struct ResponseContext<'a, M: Metric<T>, T: MetricItem, F: Fn(i64) -> usize> {
    // ctx 中的response不可直接用，先封住，按需暴露
    ctx: &'a mut CallbackContextPtr,
    metrics: &'a Arc<M>,
    dist_fn: F,
    _mark: PhantomData<T>,
}

impl<'a, M: Metric<T>, T: MetricItem, F: Fn(i64) -> usize> ResponseContext<'a, M, T, F> {
    pub(super) fn new(ctx: &'a mut CallbackContextPtr, metrics: &'a Arc<M>, dist_fn: F) -> Self {
        Self {
            ctx,
            metrics,
            dist_fn,
            _mark: Default::default(),
        }
    }
}

impl<'a, M: Metric<T>, T: MetricItem, F: Fn(i64) -> usize> Commander<M, T>
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
    #[inline(always)]
    fn metric(&self) -> &M {
        &self.metrics
    }
    fn ctx(&self) -> u64 {
        self.ctx.flag()
    }
}
