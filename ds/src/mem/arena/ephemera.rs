use std::{
    fmt::{Debug, Formatter},
    mem::MaybeUninit,
    ptr::{self, NonNull},
    sync::atomic::{AtomicUsize, Ordering::*},
};
// 当对象生命周期非常短时，可以使用 Arena 来分配内存，避免频繁的内存分配和释放。
// 一共会分配 (ITEM_NUM * LAYOUT).next_power_of_two() 个字节的内存，其中 ITEM_NUM 是对象的数量，LAYOUT 是对象的大小。
// 把这块健在分为两个部分。
// 1. 每个部分有ITEM_NUM / 2个对象；
// 2. 每个部分有一个指针，alloc_ptr，指向下一个可用的对象。
// 3. 每个部分有一个计数器，dealloc_num，表示已经释放的对象的数量。
// 4. 如果dealloc_num == ITEM_NUM / 2，表示这个部分的所有对象都已经释放，可以重用。
// idx表示当前使用的部分，如果idx == 0，表示使用第一部分，否则使用第二部分。
// 如果两个部分都已经使用完，则使用系统分配器。
pub struct Ephemera<T, const ITEM_NUM: usize> {
    chunk: [MaybeUninit<T>; ITEM_NUM],
    idx: AtomicUsize,
    dealloc_num: [AtomicUsize; 2],
    alloc_num: [AtomicUsize; 2],
}

impl<T, const ITEM_NUM: usize> Ephemera<T, ITEM_NUM> {
    const HALF_ITEM_NUM: usize = (ITEM_NUM + 1) / 2;
    const ITEM_NUM: usize = Self::HALF_ITEM_NUM * 2;
    pub const fn new() -> Self {
        let chunk = unsafe { MaybeUninit::<[MaybeUninit<T>; ITEM_NUM]>::uninit().assume_init() };
        Self {
            chunk,
            idx: AtomicUsize::new(0),
            dealloc_num: [AtomicUsize::new(0), AtomicUsize::new(0)],
            alloc_num: [AtomicUsize::new(0), AtomicUsize::new(0)],
        }
    }
    #[inline]
    fn mut_ptr(&self, pos: usize) -> &mut MaybeUninit<T> {
        unsafe { &mut *(self.chunk.get_unchecked(pos) as *const _ as *mut _) }
    }
    #[inline]
    pub fn alloc(&self, t: T) -> NonNull<T> {
        //println!("alloc: {:?}", self);
        unsafe {
            let ptr = match self.take_position() {
                Some(pos) => {
                    //println!("alloc in cache: {} => {:?}", pos, self);
                    assert!(pos < Self::ITEM_NUM);
                    self.mut_ptr(pos).write(t)
                }
                None => Box::into_raw(Box::new(t)),
            };
            NonNull::new_unchecked(ptr)
        }
    }
    #[inline]
    pub fn dealloc(&self, t: NonNull<T>) {
        let idx = self.index(t);
        //println!("dealloc idx:{}", idx);
        if idx <= 1 {
            unsafe { ptr::drop_in_place(t.as_ptr()) };
            let num = self.dealloc_num[idx].fetch_add(1, AcqRel);
            //println!("dealloc in cache:{}", num);
            assert!(num < Self::HALF_ITEM_NUM, "dealloc:{} {:?}", num, self);
            if num + 1 == Self::HALF_ITEM_NUM {
                //println!("{} cleared: {:?}", idx, self);
                self.dealloc_num[idx].fetch_sub(num + 1, AcqRel);
                assert!(
                    self.alloc_num[idx].load(Acquire) >= Self::HALF_ITEM_NUM,
                    "{:?}",
                    self
                );
                self.alloc_num[idx].store(0, Release);
                // 如果另外一个部分已经分配完，则变更idx
                if self.alloc_num[1 - idx].load(Acquire) >= Self::HALF_ITEM_NUM {
                    self.idx.store(idx, Release);
                }
            }
        } else {
            let _ = unsafe { Box::from_raw(t.as_ptr()) };
        }
    }
    // 当前对象是否在 chunk 中。
    // 0：第一部分；1：第二部分。>=2 表示不在 chunk 中。
    #[inline(always)]
    fn index(&self, ptr: NonNull<T>) -> usize {
        let ptr = ptr.as_ptr() as *const _;
        let chunk = self.chunk[0].as_ptr();
        unsafe {
            if ptr >= chunk && ptr < chunk.add(Self::ITEM_NUM) {
                if ptr < chunk.add(Self::HALF_ITEM_NUM) {
                    0
                } else {
                    1
                }
            } else {
                2
            }
        }
    }
    #[inline(always)]
    fn take_position(&self) -> Option<usize> {
        let idx = self.idx.load(Acquire);
        let pos = self.alloc_num[idx].fetch_add(1, AcqRel);
        //println!("take_position: {} idx:{}", pos, idx);
        if pos < Self::HALF_ITEM_NUM {
            let pos = pos + idx * Self::HALF_ITEM_NUM;
            return Some(pos);
        }
        //println!("idx {} run out of space", idx);
        let idx = 1 - idx;
        let pos = self.alloc_num[idx].fetch_add(1, AcqRel);
        if pos < Self::HALF_ITEM_NUM {
            // 如果另外一个部分已经分配完，则变更idx
            if pos == 0 || pos + 1 == Self::HALF_ITEM_NUM {
                self.idx.store(idx, Release);
            }
            let pos = pos + idx * Self::HALF_ITEM_NUM;
            //println!("alloc in cache: {} => {} {:?}", idx, pos, self);
            return Some(pos);
        }
        //println!("alloc in heap {:?}", self);
        HEAP.fetch_add(1, AcqRel);
        None
    }
}
pub static HEAP: AtomicUsize = AtomicUsize::new(0);

impl<T, const ITEM_NUM: usize> Drop for Ephemera<T, ITEM_NUM> {
    fn drop(&mut self) {}
}
impl<T, const ITEM_NUM: usize> Default for Ephemera<T, ITEM_NUM> {
    fn default() -> Self {
        Self::new()
    }
}
impl<T, const ITEM_NUM: usize> Debug for Ephemera<T, ITEM_NUM> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ephemera")
            .field("idx", &self.idx.load(Acquire))
            .field("alloc_num", &self.alloc_num)
            .field("dealloc_num", &self.dealloc_num)
            .finish()
    }
}

unsafe impl<T, const ITEM_NUM: usize> Sync for Ephemera<T, ITEM_NUM> {}
