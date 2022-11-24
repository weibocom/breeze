use std::{marker::PhantomData, ptr::NonNull, sync::Arc};

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
            &mut ResponseContext(self, &mut res, metric, Default::default()),
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

pub struct ResponseContext<'a, M: Metric<T>, T: std::ops::AddAssign<i64>>(
    pub(super) &'a mut CallbackContextPtr,
    pub &'a mut Command,
    pub &'a mut Arc<M>,
    pub PhantomData<T>,
);
impl<'a, M: Metric<T>, T: std::ops::AddAssign<i64>> Commander for ResponseContext<'a, M, T> {
    #[inline]
    fn request_mut(&mut self) -> &mut HashedCommand {
        self.0.request_mut()
    }
    #[inline]
    fn request(&self) -> &HashedCommand {
        self.0.request()
    }
    #[inline]
    fn response(&self) -> &Command {
        self.1
    }
    #[inline]
    fn response_mut(&mut self) -> &mut Command {
        self.1
    }
}
impl<'a, M: Metric<T>, T: std::ops::AddAssign<i64>> Metric<T> for ResponseContext<'a, M, T> {
    #[inline]
    fn get(&self, name: MetricName) -> &mut T {
        self.2.get(name)
    }
}
