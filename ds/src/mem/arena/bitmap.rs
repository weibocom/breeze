use std::{
    alloc::{self, Layout},
    ptr::NonNull,
    sync::atomic::{AtomicIsize, AtomicU64, AtomicUsize, Ordering::*},
};

const MIN_BUF_SIZE: usize = if cfg!(debug_assertions) { 2048 } else { 2048 };
const BITS: usize = std::mem::size_of::<u64>() * 8;

// 基于bitmap进行大块内存分配，BufferSize通常是1k的倍数，比如4k，8k，16k等等
// 这是一个低频率的操作，可以使用乐观锁来实现
// 降低内存碎片
#[derive(Clone, Copy)]
pub struct BitmapBufferArena;

impl BitmapBufferArena {
    #[inline]
    pub fn new(&self, size: usize) -> SizedBuffer {
        BUFFER_ARENA.alloc(size)
    }
}

pub enum Source {
    Heap,
    Arena(u8),
}
pub struct SizedBuffer {
    ptr: NonNull<u8>,
    size: usize,
    src: Source,
}

impl SizedBuffer {
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }
}

impl Drop for SizedBuffer {
    fn drop(&mut self) {
        match self.src {
            Source::Heap => unsafe {
                alloc::dealloc(
                    self.ptr.as_ptr(),
                    Layout::from_size_align_unchecked(self.size, 1),
                );
            },
            Source::Arena(idx) => {
                BUFFER_ARENA.dealloc(self.ptr, self.size, idx as usize);
            }
        }
    }
}

fn build_one(idx: usize, total_size: usize) -> RwLock<Vec<Chunk>> {
    vec![Chunk::with_index(idx, total_size)].into()
}
use spin::RwLock;
#[ctor::ctor]
static BUFFER_ARENA: BitmapBufferArenaInner = BitmapBufferArenaInner {
    slots: [
        build_one(0, 4 * 1024 * 1024),
        build_one(1, 2 * 1024 * 1024),
        build_one(2, 2 * 1024 * 1024),
        build_one(3, 2 * 1024 * 1024),
        build_one(4, 2 * 1024 * 1024),
        build_one(5, 2 * 1024 * 1024),
        build_one(6, 2 * 1024 * 1024),
        build_one(7, 2 * 1024 * 1024),
        build_one(8, 2 * 1024 * 1024),
        build_one(9, 2 * 1024 * 1024),
    ],
};

struct BitmapBufferArenaInner {
    // 第i个元素分配MIN_BUF_SIZE * 2^i大小
    // 小于 MIN_BUF_SIZE * 2^i大小的分配
    slots: [spin::RwLock<Vec<Chunk>>; 10],
}

impl BitmapBufferArenaInner {
    pub fn alloc(&self, size: usize) -> SizedBuffer {
        let i = (size.max(MIN_BUF_SIZE) / MIN_BUF_SIZE).trailing_zeros() as usize;
        if i >= self.slots.len() {
            let ptr = unsafe { alloc::alloc(Layout::from_size_align_unchecked(size, 1)) };
            return SizedBuffer {
                ptr: NonNull::new(ptr).unwrap(),
                size,
                src: Source::Heap,
            };
        }

        let slot = self.slots[i].read();
        for (si, chunk) in slot.iter().enumerate() {
            if let Some(ptr) = chunk.alloc() {
                return SizedBuffer {
                    ptr,
                    size,
                    src: Source::Arena(si as u8),
                };
            }
        }
        drop(slot);
        let mut slot = self.slots[i].write();
        let exists = slot.first().expect("slot is empty");
        let new = Chunk::new(exists.item_size, exists.count);
        let ptr = new.alloc().unwrap();
        slot.push(new);
        return SizedBuffer {
            ptr,
            size,
            src: Source::Arena(slot.len() as u8 - 1),
        };
    }
    fn dealloc(&self, ptr: NonNull<u8>, size: usize, idx: usize) {
        let slot_idx = (size.max(MIN_BUF_SIZE) / MIN_BUF_SIZE).trailing_zeros() as usize;
        assert!(slot_idx < self.slots.len());
        self.slots[slot_idx].read()[idx].dealloc(ptr);
    }
}

// 每个chunke包含count个元素，每个元素大小为SIZE
pub struct Chunk {
    // 0表示空闲，1表示已分配
    bitmaps: Vec<AtomicU64>,
    data: *mut u8,
    item_size: usize,
    count: usize,
    // 第idx个bitmap可能有空闲的元素.
    next_idx: AtomicUsize,
    // 可以分配的数量
    free: AtomicIsize,
}

impl Chunk {
    fn with_index(idx: usize, total_size: usize) -> Self {
        let item_size = MIN_BUF_SIZE * (1 << idx);
        let count = total_size / item_size;
        assert_eq!(count * item_size, total_size,);
        Self::new(item_size, count)
    }
    pub fn new(item_size: usize, count: usize) -> Self {
        // item_size是MIN_BUF_SIZE的2的指数倍
        assert!((item_size / MIN_BUF_SIZE).is_power_of_two());
        // count是64的倍数
        assert!(count > 0, "count must be greater than 0");

        let bits_count = count / BITS;
        let mut bitmaps = Vec::with_capacity(bits_count);
        for _i in 0..bits_count {
            bitmaps.push(AtomicU64::new(0));
        }
        // count可能不是64的倍数，所以最后一个bitmap可能不是0
        if count % BITS != 0 {
            let shift = count % BITS;
            let last = !0u64 >> shift << shift;
            bitmaps.push(last.into());
        }

        let data = unsafe {
            let layout = Layout::from_size_align(item_size * count, MIN_BUF_SIZE).unwrap();
            alloc::alloc(layout) as *mut u8
        };
        Self {
            item_size,
            count,
            bitmaps,
            data,
            next_idx: 0.into(),
            free: (count as isize).into(),
        }
    }
    #[inline]
    pub fn alloc(&self) -> Option<NonNull<u8>> {
        if self.free.fetch_sub(1, AcqRel) <= 0 {
            // 预留失败，释放
            self.free.fetch_add(1, AcqRel);
            return None;
        } else {
            let mut idx = self.next_idx.load(Acquire);
            for _ in 0..self.bitmaps.len() {
                if idx == self.bitmaps.len() {
                    idx = 0;
                }
                let bitmap = &self.bitmaps[idx];
                loop {
                    let bits = bitmap.load(Acquire);
                    if bits == !0u64 {
                        // 当前bitmap已经没有空闲的元素了
                        break;
                    }
                    let pos = unsafe { bits.flip_zero_bit() };
                    if bitmap
                        .compare_exchange(bits, bits | (1 << pos), AcqRel, Acquire)
                        .is_ok()
                    {
                        self.next_idx.store(idx, Release);
                        let oft = idx * BITS + pos;
                        let ptr = unsafe { self.data.add(oft * self.item_size) };
                        return Some(NonNull::new(ptr).unwrap());
                    }
                }
                idx += 1;
            }
            panic!("should not reach here:{:?}", self);
        }
    }
    pub fn dealloc(&self, p: NonNull<u8>) {
        let ptr = p.as_ptr();
        let idx = (ptr as usize - self.data as usize) / self.item_size;
        let bitmap = &self.bitmaps[idx / BITS];
        let pos = idx % BITS;
        loop {
            let bits = bitmap.load(Acquire);
            // 释放的元素必须是已分配的
            assert!((bits & (1 << pos)) != 0);
            if bitmap
                .compare_exchange(bits, bits & !(1 << pos), AcqRel, Acquire)
                .is_ok()
            {
                self.free.fetch_add(1, AcqRel);
                break;
            }
        }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        unsafe {
            let layout =
                Layout::from_size_align(self.item_size * self.count, MIN_BUF_SIZE).unwrap();
            alloc::dealloc(self.data as *mut u8, layout);
        }
    }
}

trait BitOps {
    unsafe fn flip_zero_bit(&self) -> usize;
}

impl BitOps for u64 {
    // 翻转第一个0位，如果没有，则panic
    unsafe fn flip_zero_bit(&self) -> usize {
        let mut v = *self;
        for i in 0..64 {
            if v & 1 == 0 {
                return i;
            }
            v >>= 1;
        }
        panic!("zero bit not found in {}", self);
    }
}

use std::fmt;
impl fmt::Debug for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Chunk")
            .field("item_size", &self.item_size)
            .field("count", &self.count)
            .field("next_idx", &self.next_idx.load(Acquire))
            .field("free", &self.free.load(Acquire))
            .finish()
    }
}
