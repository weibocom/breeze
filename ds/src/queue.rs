use std::collections::LinkedList;

// queue在扩缩容的时候，原有的数据不移动。
// 调用者拿到item的指针后，在数据被pop之前，可以安全的引用。
// 只支持appendonly.
pub struct PinnedQueue<T> {
    head: usize,        // fix的head位置
    tail: usize,        // fix的tail位置
    cap: u32,           // fix的容量。power of two
    fix: *mut T,        // 固定长度的queue的地址
    ext: LinkedList<T>, // 超过固定长度后，数据写入到ext中。
    fix_head: bool,     // 当前队列的头是否在fix上.
    fix_tail: bool,     // 当前队列的尾部是否在tail上。
}

impl<T> PinnedQueue<T> {
    pub fn with_capacity(cap: usize) -> Self {
        let mut fix = Vec::with_capacity(cap);
        let ptr = fix.as_mut_ptr();
        let _ = std::mem::ManuallyDrop::new(fix);
        Self {
            head: 0,
            tail: 0,
            cap: cap as u32,
            fix: ptr,
            ext: LinkedList::new(),
            fix_head: true,
            fix_tail: true,
        }
    }
    #[inline(always)]
    pub fn new() -> Self {
        Self::with_capacity(8)
    }
    // 把数据推入back，并且返回原有的引用
    #[inline(always)]
    pub fn push_back(&mut self, t: T) -> &mut T {
        if self.fix_tail {
            let ptr = self.tailer();
            unsafe { ptr.write(t) };
            self.tail += 1;
            if self.is_full() {
                // 后续的push_back往ext里面写
                self.fix_tail = false;
            }
            unsafe { &mut *ptr }
        } else {
            self.ext.push_back(t);
            self.ext.back_mut().expect("ext back mut")
        }
    }
    #[inline(always)]
    pub fn pop_front(&mut self) -> Option<T> {
        //println!("pop front:{}", self);
        if self.len() == 0 {
            None
        } else {
            unsafe { Some(self.pop_front_unchecked()) }
        }
    }

    #[inline(always)]
    pub unsafe fn pop_front_unchecked(&mut self) -> T {
        debug_assert_ne!(self.len(), 0);
        if self.fix_head {
            debug_assert_ne!(self.fix_len(), 0);
            let t = self.header().read();
            self.head += 1;
            if self.fix_empty() {
                // 头到了ext
                if self.ext.len() > 0 {
                    self.fix_head = false;
                    self.grow();
                } else {
                    self.fix_tail = true;
                }
            }
            t
        } else {
            debug_assert_ne!(self.ext.len(), 0);
            let t = self.ext.pop_front().expect("take front");
            if self.ext.len() == 0 {
                debug_assert_eq!(self.fix_len(), 0);
                self.fix_head = true;
                self.fix_tail = true;
            }
            t
        }
    }
    #[inline(always)]
    unsafe fn front_mut_unchecked(&mut self) -> &mut T {
        debug_assert_ne!(self.len(), 0);
        if self.fix_head {
            debug_assert_ne!(self.fix_len(), 0);
            &mut *self.header()
        } else {
            debug_assert_ne!(self.ext.len(), 0);
            self.ext.front_mut().expect("ext front")
        }
    }
    #[inline(always)]
    pub unsafe fn front_unchecked(&self) -> &T {
        debug_assert_ne!(self.len(), 0);
        if self.fix_head {
            debug_assert_ne!(self.fix_len(), 0);
            &*self.header()
        } else {
            debug_assert_ne!(self.ext.len(), 0);
            self.ext.front().expect("ext front")
        }
    }
    #[inline(always)]
    pub fn front_mut(&mut self) -> Option<&mut T> {
        if self.len() == 0 {
            None
        } else {
            unsafe { Some(self.front_mut_unchecked()) }
        }
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.fix_len() + self.ext.len()
    }
    #[inline(always)]
    fn mask(&self, pos: usize) -> usize {
        (self.cap as usize - 1) & pos
    }
    #[inline(always)]
    fn ptr(&self, pos: usize) -> *mut T {
        unsafe { self.fix.offset(self.mask(pos) as isize) }
    }
    #[inline(always)]
    fn tailer(&mut self) -> *mut T {
        self.ptr(self.tail)
    }
    #[inline(always)]
    fn header(&self) -> *mut T {
        self.ptr(self.head)
    }
    #[inline(always)]
    fn fix_len(&self) -> usize {
        self.tail - self.head
    }
    // 检查fix是否已满
    #[inline(always)]
    fn is_full(&self) -> bool {
        self.fix_len() >= self.cap as usize
    }
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    #[inline(always)]
    fn fix_empty(&self) -> bool {
        self.head == self.tail
    }
    #[inline(always)]
    fn cap(&self) -> usize {
        self.cap as usize
    }

    // fix为空时才能grow, 每次grow后会重置head与tail指针
    #[inline(always)]
    fn grow(&mut self) {
        debug_assert_eq!(self.fix_len(), 0);
        debug_assert_ne!(self.ext.len(), 0);
        // drop old
        let _v = unsafe { Vec::from_raw_parts(self.fix, 0, self.cap as usize) };
        self.cap = self.cap * 2;
        let mut new = Vec::with_capacity(self.cap() * 2);
        self.fix = new.as_mut_ptr();
        let _ = std::mem::ManuallyDrop::new(new);
        self.head = 0;
        self.tail = 0;
        //println!("grown {}", self);
    }
}

unsafe impl<T> Send for PinnedQueue<T> {}
unsafe impl<T> Sync for PinnedQueue<T> {}

impl<T> Drop for PinnedQueue<T> {
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if !self.fix_empty() {
                let head = self.mask(self.head);
                let tail = self.mask(self.tail);
                use std::ptr;
                if head < tail {
                    // 说明内存是连续的。
                    ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                        self.header(),
                        self.fix_len(),
                    ));
                } else {
                    // 分段释放
                    // 1. 从header到末尾
                    ptr::drop_in_place(ptr::slice_from_raw_parts_mut(
                        self.header(),
                        self.cap() - head,
                    ));
                    // 2. 从开始到tail
                    ptr::drop_in_place(ptr::slice_from_raw_parts_mut(self.fix, tail));
                }
            }
        }
    }
}

use std::fmt::{self, Display, Formatter};
impl<T> Display for PinnedQueue<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "len:{} fix:(cap:{} len:{} head:{} tail:{}) ext-len:{} direction:(head:{}, tail:{})",
            self.len(),
            self.cap,
            self.fix_len(),
            self.head,
            self.tail,
            self.ext.len(),
            self.fix_head,
            self.fix_tail,
        )
    }
}
