use std::ptr::NonNull;

use ds::arena::{Allocator, Arena, Ephemera};

use crate::CallbackContext;

pub(crate) struct CallbackContextArena(Arena<CallbackContext, GlobalArena>);

// 首先从localcache分配；
// 再从globalcache分配
// 最后从默认的分配器分配
impl CallbackContextArena {
    pub(crate) fn with_cache(cache: usize) -> Self {
        Self(Arena::with_cache(cache, GlobalArena))
    }
    #[inline(always)]
    pub(crate) fn alloc(&mut self, t: CallbackContext) -> NonNull<CallbackContext> {
        self.0.alloc(t)
    }
    #[inline(always)]
    pub(crate) fn dealloc(&mut self, ptr: NonNull<CallbackContext>) {
        self.0.dealloc(ptr)
    }
}

// 占用内存约为 20480 * 256 = 5.2M
type Global = Ephemera<CallbackContext, 20480>;

#[ctor::ctor]
static GLOBAL_CACHE: Global = Global::new();

struct GlobalArena;

impl Allocator<CallbackContext> for GlobalArena {
    #[inline(always)]
    fn alloc(&self, t: CallbackContext) -> NonNull<CallbackContext> {
        GLOBAL_CACHE.alloc(t)
    }
    #[inline(always)]
    fn dealloc(&self, ptr: NonNull<CallbackContext>) {
        GLOBAL_CACHE.dealloc(ptr)
    }
}
