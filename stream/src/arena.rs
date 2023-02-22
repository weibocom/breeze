use std::ptr::NonNull;

use ds::arena::{Allocator, Arena, Ephemera};

use crate::CallbackContext;

type L2Cache = GlobalArena;
//type L2Cache = ds::arena::Heap;

pub(crate) struct CallbackContextArena(Arena<CallbackContext, L2Cache>);

// 首先从localcache分配；
// 再从globalcache分配
// 最后从默认的分配器分配
impl CallbackContextArena {
    pub(crate) fn with_cache(cache: usize) -> Self {
        Self(Arena::with_cache(cache, L2Cache {}))
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

type Global = Ephemera<CallbackContext>;

// 占用内存约为 32768 * 192 = 6.2Mb
#[ctor::ctor]
static GLOBAL_CACHE: Global = Global::with_cache(1 << 15);

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
