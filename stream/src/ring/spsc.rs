use std::cell::RefCell;
use std::ptr::copy_nonoverlapping as copy;
use std::slice::from_raw_parts;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use cache_line_size::CacheAligned;
use lockfree::channel::spsc::{create, Receiver, Sender};

use futures::ready;
use spin::Mutex;

unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

pub struct RingBuffer {
    data: *mut u8,
    len: usize,
    read: CacheAligned<AtomicUsize>,
    write: CacheAligned<AtomicUsize>,
    //waker_status: AtomicBool,
    //waker: RefCell<Option<Waker>>,
    waker: Mutex<Option<Waker>>,
}

impl RingBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        debug_assert_eq!(cap, cap.next_power_of_two());
        let mut data = vec![0u8; cap];
        let ptr = data.as_mut_ptr();
        std::mem::forget(data);

        //let (sender, receiver) = create();
        Self {
            data: ptr,
            len: cap,
            read: CacheAligned(AtomicUsize::new(0)),
            write: CacheAligned(AtomicUsize::new(0)),
            //sender: sender,
            //receiver: receiver,
            //waker: Default::default(),
            //waker_status: AtomicBool::new(false),
            waker: Mutex::new(None),
        }
    }
    pub fn into_split(self) -> (RingBufferWriter, RingBufferReader) {
        let buffer = Arc::new(self);
        (
            RingBufferWriter::from(buffer.clone()),
            RingBufferReader::from(buffer.clone()),
        )
    }
    // 读和写同时只会出现一个notify. TODO 待验证
    fn notify(&self) {
        let mut waker = self.waker.lock();
        if let Some(w) = waker.take() {
            println!(
                "spsc: notyfied: read:{} write:{}",
                self.read.0.load(Ordering::Acquire),
                self.write.0.load(Ordering::Acquire)
            );
            w.wake()
        }
        //if self.waker_status.load(Ordering::Acquire) {
        //    if let Some(waker) = self.waker.borrow_mut().take() {
        //        self.waker_status.store(false, Ordering::Release);
        //        waker.wake();
        //        println!("spsc: entering pending mode");
        //    }
        //}
    }
    fn wake_and_pending(&self, cx: &mut Context) -> Poll<()> {
        println!(
            "spsc: entering pending mode: read:{} write:{}",
            self.read.0.load(Ordering::Acquire),
            self.write.0.load(Ordering::Acquire)
        );
        let mut waker = self.waker.lock();
        let read = self.read.0.load(Ordering::Acquire);
        let write = self.write.0.load(Ordering::Acquire);
        if read != write {
            println!(
                "spsc: entering pending mode, but read{} write:{}",
                read, write
            );
            return Poll::Ready(());
        }
        // 拿到锁了，进行double check
        waker.take().map(|w| {
            println!("spsc: notified in wake pending");
            w.wake()
        });
        *waker = Some(cx.waker().clone());
        Poll::Pending
        //*self.waker.borrow_mut() = Some(cx.waker().clone());
        //self.waker_status.store(true, Ordering::Release);
    }
}

impl Drop for RingBuffer {
    fn drop(&mut self) {
        let _data = unsafe { Vec::from_raw_parts(self.data, 0, self.len) };
    }
}

pub struct RingBufferWriter {
    buffer: Arc<RingBuffer>,
    write: usize,
    mask: usize,
}

impl RingBufferWriter {
    fn from(buffer: Arc<RingBuffer>) -> Self {
        let mask = buffer.len - 1;
        Self {
            buffer: buffer,
            mask: mask,
            write: 0,
        }
    }
    // 写入b到buffer。
    // true: 写入成功，false：写入失败。
    // 要么全部成功，要么全部失败。不会写入部分字节
    pub fn put_slice(&mut self, b: &[u8]) -> bool {
        let read = self.buffer.read.0.load(Ordering::Acquire);
        let available = self.buffer.len - (self.write - read);
        debug_assert!(available <= self.buffer.len);
        println!(
            "spsc: try write data. read:{} write:{} buffer write:{} available:{} len:{}",
            read,
            self.write,
            self.buffer.write.0.load(Ordering::Acquire),
            available,
            b.len()
        );
        if available < b.len() {
            false
        } else {
            let oft_write = self.write & self.mask;
            let oft_read = read & self.mask;
            let offset = oft_write.max(oft_read);
            let n = (self.buffer.len - offset).min(b.len());
            unsafe {
                copy(b.as_ptr(), self.buffer.data.offset(offset as isize), n);
            }
            if b.len() > n && available > n {
                // 说明写入到buffer末尾，空间够，还有未写完的数据
                // 会从0写入
                unsafe {
                    copy(b.as_ptr().offset(n as isize), self.buffer.data, b.len() - n);
                }
            }
            self.write += b.len();
            self.buffer.write.0.store(self.write, Ordering::Release);
            true
        }
    }
    pub fn poll_put_slice(&mut self, cx: &mut Context, b: &[u8]) -> Poll<()> {
        if self.put_slice(b) {
            self.buffer.notify();
            Poll::Ready(())
        } else {
            // 空间不足
            println!("spsc: no space to write, entering waiting mode. ");
            self.buffer.notify();
            self.buffer.wake_and_pending(cx)
        }
    }
}

pub struct RingBufferReader {
    read: usize,
    mask: usize,
    buffer: Arc<RingBuffer>,
}

impl RingBufferReader {
    fn from(buffer: Arc<RingBuffer>) -> Self {
        let mask = buffer.len - 1;
        Self {
            buffer: buffer,
            read: 0,
            mask: mask,
        }
    }
    // 如果ringbuffer到达末尾，则只返回到末尾的slice
    pub fn next(&self) -> Option<&[u8]> {
        let write = self.buffer.write.0.load(Ordering::Acquire);
        if self.read == write {
            println!(
                "spsc: no data to read.buffer read:{} read:{} write:{}",
                self.buffer.read.0.load(Ordering::Acquire),
                self.read,
                write
            );
            None
        } else {
            debug_assert!(self.read < write);
            let oft_start = self.read & self.mask;
            let oft_write = write & self.mask;
            let n = if oft_write > oft_start {
                oft_write - oft_start
            } else {
                self.buffer.len - oft_start
            };
            println!("spsc poll next. read:{} write:{} n:{}", self.read, write, n);
            unsafe {
                Some(from_raw_parts(
                    self.buffer.data.offset(oft_start as isize),
                    n,
                ))
            }
        }
    }
    pub fn consume(&mut self, n: usize) {
        self.read += n;
        self.buffer.read.0.store(self.read, Ordering::Release);
        self.buffer.notify();
    }
    // 没有数据进入waiting状态。等put唤醒
    pub fn poll_next(&mut self, cx: &mut Context) -> Poll<&[u8]> {
        if let Some(data) = self.next() {
            Poll::Ready(data)
        } else {
            // 没有数据了
            ready!(self.buffer.wake_and_pending(cx));
            let data = self.next().expect("spsc not entering pending status");
            Poll::Ready(data)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::RingBuffer;
    use rand::Rng;
    fn rnd_bytes(n: usize) -> Vec<u8> {
        (0..n).map(|_| rand::random::<u8>()).collect()
    }
    #[test]
    fn test_spsc_ring_buffer() {
        let cap = 32;
        let (mut writer, mut reader) = RingBuffer::with_capacity(cap).into_split();
        // 场景0： 一次写满
        let b = rnd_bytes(cap);
        assert!(writer.put_slice(&b));
        assert!(!writer.put_slice(&[b'0']));
        let r = reader.next();
        assert!(r.is_some());
        let r = r.unwrap();
        assert_eq!(&b, r);
        reader.consume(b.len());

        // 场景1: 两次写满
        let b0 = rnd_bytes(cap / 2 - 1);
        let b1 = rnd_bytes(cap / 2 + 1);
        assert!(writer.put_slice(&b0));
        assert!(writer.put_slice(&b1));
        assert!(!writer.put_slice(&[b'0']));
        let b = reader.next();
        assert!(b.is_some());
        let b = b.unwrap();
        assert_eq!(&b0, &b[..b0.len()]);
        assert_eq!(&b1, &b[b0.len()..]);
        reader.consume(b.len());

        // 场景2： 多次写入，多次读取，不对齐
        let b0 = rnd_bytes(cap / 3);
        writer.put_slice(&b0);
        reader.consume(b0.len());
        // 写入字节数大于 cap - b0.len()
        let b0 = rnd_bytes(cap - 2);
        assert!(writer.put_slice(&b0));
        // 当前内容分成两断
        let r1 = reader.next();
        assert!(r1.is_some());
        let r1 = r1.unwrap();
        assert_eq!(&b0[..r1.len()], r1);
        let r1l = r1.len();
        reader.consume(r1.len());
        let r2 = reader.next();
        assert!(r2.is_some());
        let r2 = r2.unwrap();
        assert_eq!(&b0[r1l..], r2);
        reader.consume(r2.len());
        let r3_none = reader.next();
        assert!(r3_none.is_none());

        // 场景3
        // 从1-cap个字节写入并读取
        for i in 1..=cap {
            let b = rnd_bytes(i);
            writer.put_slice(&b);
            let r1 = reader.next();
            assert!(r1.is_some());
            let r1 = r1.unwrap();
            let r1_len = r1.len();
            assert_eq!(&b[0..r1_len], r1);
            reader.consume(r1_len);
            if r1_len != b.len() {
                let r2 = reader.next();
                assert!(r2.is_some());
                let r2 = r2.unwrap();
                assert_eq!(&b[r1_len..], r2);
                reader.consume(r2.len());
            }
        }
    }
}
