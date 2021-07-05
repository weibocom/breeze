use std::cell::RefCell;
use std::io::{Error, ErrorKind, Result};
use std::ptr::copy_nonoverlapping as copy;
use std::slice::from_raw_parts;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use cache_line_size::CacheAligned;

unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

#[repr(u8)]
#[derive(Clone, Copy)]
pub enum Status {
    Ok = 0u8,
    ReadPending = 1,
    WritePending = 2,
    Lock,
    Close,
}

const STATUSES: [Status; 5] = [
    Status::Ok,
    Status::ReadPending,
    Status::WritePending,
    Status::Lock,
    Status::Close,
];

impl PartialEq for Status {
    fn eq(&self, other: &Self) -> bool {
        *self as u8 == *other as u8
    }
}
impl PartialEq<u8> for Status {
    fn eq(&self, other: &u8) -> bool {
        *self as u8 == *other
    }
}
impl PartialEq<Status> for u8 {
    fn eq(&self, other: &Status) -> bool {
        *self == *other as u8
    }
}

impl From<u8> for Status {
    fn from(status: u8) -> Self {
        debug_assert!(status <= Status::Lock as u8);
        STATUSES[status as usize]
    }
}

pub struct RingBuffer {
    data: *mut u8,
    len: usize,
    read: CacheAligned<AtomicUsize>,
    write: CacheAligned<AtomicUsize>,
    waker_status: AtomicU8,
    closed: AtomicBool,
    // 0: ReadPending, 1: WritePending
    wakers: [RefCell<Option<Waker>>; 2],
}

impl RingBuffer {
    pub fn with_capacity(cap: usize) -> Self {
        debug_assert_eq!(cap, cap.next_power_of_two());
        let mut data = vec![0u8; cap];
        let ptr = data.as_mut_ptr();
        std::mem::forget(data);

        Self {
            data: ptr,
            len: cap,
            read: CacheAligned(AtomicUsize::new(0)),
            write: CacheAligned(AtomicUsize::new(0)),
            waker_status: AtomicU8::new(Status::Ok as u8),
            wakers: Default::default(),
            closed: AtomicBool::new(false),
        }
    }
    pub fn into_split(self) -> (RingBufferWriter, RingBufferReader) {
        let buffer = Arc::new(self);
        (
            RingBufferWriter::from(buffer.clone()),
            RingBufferReader::from(buffer.clone()),
        )
    }
    fn close(&self) {
        self.waker_status
            .store(Status::Close as u8, Ordering::Release);
    }
    // 读和写同时只会出现一个notify. TODO 待验证
    fn notify(&self, status: Status) {
        log::debug!("spsc: notify status:{}", status as u8);
        debug_assert!(
            status as u8 == Status::ReadPending as u8 || status as u8 == Status::WritePending as u8
        );
        if self.waker_status.load(Ordering::Acquire) == Status::Close as u8 {
            log::info!("buffer closed. no need to notify?");
            return;
        }
        if self.status_cas(status, Status::Lock) {
            // 进入到pending状态，一定会有waker
            self.wakers[status as usize - 1]
                .borrow_mut()
                .take()
                .expect("waiting status must contain waker.")
                .wake();
            log::debug!("spsc notifyed:{}", status as u8);
            let _cas = self.status_cas(Status::Lock, Status::Ok);
            debug_assert!(_cas);
            return;
        } else {
            // 说明当前状态不是需要notify的status状态，直接忽略即可
            //log::debug!("try to lock status failed");
        }
    }
    // 当前状态要进入到status状态（status只能是ReadPending或者WritePending
    fn waiting(&self, cx: &mut Context, status: Status) {
        log::debug!("spsc poll next entering waiting status:{}", status as u8);
        debug_assert!(status == Status::ReadPending || status == Status::WritePending);
        //for _ in 0..128 {
        let mut loops = 0;
        loop {
            loops += 1;
            // 异常情况，进入死循环了
            debug_assert!(loops <= 1024 * 1024);
            let old = self.waker_status.load(Ordering::Acquire);
            if old == Status::Close {
                return;
            }
            if old == Status::Ok || old == status {
                if self.status_cas(old.into(), Status::Lock) {
                    *self.wakers[status as usize - 1].borrow_mut() = Some(cx.waker().clone());
                    let _cas = self.status_cas(Status::Lock, status);
                    debug_assert!(_cas);
                    return;
                } else {
                    continue;
                }
            }

            if old == Status::Lock {
                log::debug!("waiting into status. but old status is:{}", old);
                continue;
            }

            // 运行到这，通常是下面这种场景：
            // 写阻塞，进入waiting，但瞬间所有数据都被读走，读也会被阻塞。反之一样
            // 唤醒old的
            log::warn!("try to waiting in status {}, but current status is {}. maybe both write and read enterinto Pending status mode", status as u8, old);
            self.notify(Status::from(old));
            return;
        }
    }
    fn status_cas(&self, old: Status, new: Status) -> bool {
        log::debug!("spsc status cas. old:{} new:{}", old as u8, new as u8);
        match self.waker_status.compare_exchange(
            old as u8,
            new as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => true,
            Err(status) => {
                log::debug!(
                    "spsc: try to status cas failed. old:{}, new:{} current:{}",
                    old as u8,
                    new as u8,
                    status as u8
                );
                false
            }
        }
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
    closed: bool,
}

impl RingBufferWriter {
    fn from(buffer: Arc<RingBuffer>) -> Self {
        let mask = buffer.len - 1;
        Self {
            buffer: buffer,
            mask: mask,
            write: 0,
            closed: false,
        }
    }
    // 写入b到buffer。
    // true: 写入成功，false：写入失败。
    // 要么全部成功，要么全部失败。不会写入部分字节
    pub fn put_slice(&mut self, b: &[u8]) -> Result<usize> {
        if self.closed {
            return Result::Err(Error::new(ErrorKind::BrokenPipe, "channel is closed"));
        }
        let read = self.buffer.read.0.load(Ordering::Acquire);
        let available = self.buffer.len - (self.write - read);
        debug_assert!(available <= self.buffer.len);
        debug_assert!(b.len() < self.buffer.len);
        if available < b.len() {
            Result::Ok(0 as usize)
        } else {
            let oft_write = self.write & self.mask;
            let oft_read = read & self.mask;
            let n = if oft_read > oft_write {
                b.len()
            } else {
                b.len().min(self.buffer.len - oft_write)
            };
            unsafe {
                copy(b.as_ptr(), self.buffer.data.offset(oft_write as isize), n);
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
            Result::Ok(b.len())
        }
    }
    pub fn poll_put_slice(&mut self, cx: &mut Context, b: &[u8]) -> Poll<Result<usize>> {
        let result = self.put_slice(b);
        log::debug!("poll put slice:{:?}", result);
        if result.is_ok() {
            let result_size = result.unwrap();
            if result_size == 0 {
                self.buffer.waiting(cx, Status::WritePending);
                Poll::Pending
            } else {
                log::debug!("spsc: put slice success, notify read pending");
                self.buffer.notify(Status::ReadPending);
                Poll::Ready(Result::Ok(result_size))
            }
        } else {
            self.buffer.close();
            Poll::Ready(result)
        }
    }

    pub fn close(&mut self) {
        self.closed = true;
        self.buffer.close();
    }
}
impl Drop for RingBufferWriter {
    fn drop(&mut self) {
        // 唤醒读状态的waker
        self.buffer.closed.store(true, Ordering::Release);
        self.buffer.notify(Status::ReadPending);
    }
}

pub struct RingBufferReader {
    read: usize,
    mask: usize,
    buffer: Arc<RingBuffer>,
    close: bool,
}

impl RingBufferReader {
    fn from(buffer: Arc<RingBuffer>) -> Self {
        let mask = buffer.len - 1;
        Self {
            buffer: buffer,
            read: 0,
            mask: mask,
            close: false,
        }
    }
    // 如果ringbuffer到达末尾，则只返回到末尾的slice
    pub fn next(&self) -> Result<Option<&[u8]>> {
        if self.close {
            return Err(Error::new(ErrorKind::BrokenPipe, "channel is closed"));
        }
        let write = self.buffer.write.0.load(Ordering::Acquire);
        if self.read == write {
            Result::Ok(None)
        } else {
            debug_assert!(self.read < write);
            let oft_start = self.read & self.mask;
            let oft_write = write & self.mask;
            let n = if oft_write > oft_start {
                oft_write - oft_start
            } else {
                self.buffer.len - oft_start
            };
            //log::debug!("spsc poll next. read:{} write:{} n:{}", self.read, write, n);
            unsafe {
                Result::Ok(Some(from_raw_parts(
                    self.buffer.data.offset(oft_start as isize),
                    n,
                )))
            }
        }
    }
    pub fn consume(&mut self, n: usize) {
        self.read += n;
        self.buffer.read.0.store(self.read, Ordering::Release);
        self.buffer.notify(Status::WritePending);
    }
    // 没有数据进入waiting状态。等put唤醒
    pub fn poll_next(&mut self, cx: &mut Context) -> Poll<Result<&[u8]>> {
        let result = self.next();
        if result.is_ok() {
            let poll_result = result.unwrap();
            if poll_result.is_some() {
                Poll::Ready(Result::Ok(poll_result.unwrap()))
            } else {
                self.buffer.waiting(cx, Status::ReadPending);
                Poll::Pending
            }
        } else {
            self.buffer.close();
            Poll::Ready(Result::Err(result.unwrap_err()))
        }
    }
}

impl Drop for RingBufferReader {
    fn drop(&mut self) {
        // 唤醒读状态的waker
        self.buffer.closed.store(true, Ordering::Release);
        self.buffer.notify(Status::WritePending);
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

    #[test]
    fn test_spsc_data_consistent() {
        use std::sync::Arc;
        use std::task::{Context, Poll};
        let cap = 1024 * 1024;
        let (mut writer, mut reader) = RingBuffer::with_capacity(8 * cap).into_split();
        // 场景0： 一次写满
        // 生成10 * cap的数据。一读一写
        let request_data: Arc<Vec<u8>> = Arc::new(rnd_bytes(cap));
        let mut readed_data: Vec<u8> = Vec::with_capacity(request_data.len());

        let w_data = request_data.clone();
        // 一写一读
        let w = std::thread::spawn(move || {
            let waker = futures::task::noop_waker_ref();
            let mut cx = std::task::Context::from_waker(waker);
            let min = 512usize;
            let max = 1024usize;
            let mut writes = 0;
            let mut i = min;
            while writes < w_data.len() {
                let end = (w_data.len() - writes).min(i) + writes;
                match writer.poll_put_slice(&mut cx, &w_data[writes..end]) {
                    Poll::Ready(_) => {
                        writes = end;
                        i += 1;
                        if i >= max {
                            i = min
                        }
                    }
                    Poll::Pending => {
                        std::hint::spin_loop();
                    }
                };
            }
        });
        let r = std::thread::spawn(move || {
            let waker = futures::task::noop_waker_ref();
            let mut cx = std::task::Context::from_waker(waker);
            let size = readed_data.capacity();
            while readed_data.len() < size {
                let n = match reader.poll_next(&mut cx) {
                    Poll::Ready(data) => {
                        debug_assert!(data.unwrap().len() > 0);
                        readed_data.extend_from_slice(data.unwrap());
                        data.unwrap().len()
                    }
                    Poll::Pending => {
                        std::hint::spin_loop();
                        0
                    }
                };
                if n > 0 {
                    reader.consume(n);
                }
            }
            readed_data
        });
        w.join().unwrap();
        let readed_data = r.join().unwrap();
        assert_eq!(request_data.len(), readed_data.len());
        assert_eq!(&request_data[..], &readed_data[..]);
    }
}
