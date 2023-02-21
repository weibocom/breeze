use ds::{RingBuffer, RingSlice};

fn rnd_bytes(size: usize) -> Vec<u8> {
    let data: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
    data
}

struct RandomReader(Vec<u8>);
impl ds::BuffRead for RandomReader {
    type Out = RingSlice;
    fn read(&mut self, buf: &mut [u8]) -> (usize, Self::Out) {
        let start = rand::random::<usize>() % self.0.len();
        let len = (self.0.capacity() - start).min(buf.len());
        let data = &self.0[start..start + len];
        buf[..len].copy_from_slice(data);
        (len, self.0[start..start + len].into())
    }
}

// 1. 验证简单的数据写入与读取
// 2. 验证分段的数据写入与读取
// 3. 验证随机的数据写入与读取
#[test]
fn ring_buffer_basic() {
    let cap = 32;
    let data = rnd_bytes(cap);

    // 1. 场景1：简单写入数据，然后读取。
    let rs = RingSlice::from(data.as_ptr(), cap, 0, 17);
    let mut buf = RingBuffer::with_capacity(cap);
    buf.write(&rs);
    assert_eq!(buf.len(), rs.len());
    assert!(&(buf.data()) == &data[0..17]);
    buf.consume(buf.len());
    assert_eq!(buf.len(), 0);

    let rs = RingSlice::from(data.as_ptr(), cap, 32, 32 + 32);
    let n = buf.write(&rs);
    assert_eq!(buf.len(), rs.len());
    assert_eq!(buf.len(), n);
    assert_eq!(&buf.data(), &data[..]);
    buf.consume(rs.len());
    assert_eq!(buf.len(), 0);

    // 2. 场景2：分段写入数据，然后读取。
    // 有折返的
    let rs = RingSlice::from(data.as_ptr(), cap, 27, 27 + 19);
    assert_eq!(rs.len(), 19);
    let n = buf.write(&rs);
    assert_eq!(n, rs.len());
    assert_eq!(buf.len(), rs.len());
    assert_eq!(buf.len(), 19);
    assert_eq!(buf.data(), rs);
    let rs = RingSlice::from(data.as_ptr(), cap, 32, 32 + 32);
    let n = buf.write(&rs);
    assert_eq!(n, 32 - 19);
    assert_eq!(buf.len(), 32);
    buf.consume(buf.len());
    assert_eq!(buf.len(), 0);

    // 3. 场景3：随机写入数据，然后读取。
    // 随机写入m个字节，读取n个字节，然后对比
    let runs = 1000;
    for i in 0..runs {
        // 随机写入数据
        let start = (rand::random::<u32>()) as usize;
        let end = start + rand::random::<u32>() as usize % cap;
        let rs = RingSlice::from(data.as_ptr(), cap, start, end);
        let m = buf.write(&rs);
        // 要么全部写入，要么没有剩余
        assert!(rs.len() == m || buf.available() == 0);
        // 对比新写入的数据一致性
        let writtened = buf.data().slice(buf.len() - m, m);
        let src = rs.slice(0, m);
        assert_eq!(writtened, src, "{}-th", i);
        // 随机消费n条数据
        if buf.len() > 0 {
            let n = rand::random::<u32>() as usize % buf.len();
            buf.consume(n);
        }
    }

    // 随机从reader读取数据
    let runs = 1000;
    let mut reader = RandomReader(rnd_bytes(cap * 32));
    for i in 0..runs {
        let writtened = buf.copy_from(&mut reader);
        let len = writtened.len();
        let src = buf.data().slice(buf.len() - len, len);
        assert_eq!(writtened, src, "{}-th", i);
        // 随机消费n条数据
        let n = rand::random::<u32>() as usize % buf.len();
        buf.consume(n);
    }
    buf.consume(buf.len());
}

#[test]
fn ring_buffer_resize() {
    //let mut rrb = ds::ResizedRingBuffer::from(256, 4 * 1024, 1024);
    //assert_eq!(1024, rrb.cap());
    //assert_eq!(0, rrb.len());
    //// 一次写满
    //let buf = rrb.as_mut_bytes();
    //assert_eq!(buf.len(), 1024);
    //assert_eq!(rrb.len(), 0);
    //assert_eq!(rrb.cap(), 1024);
    //rrb.advance_write(1024);
    //assert_eq!(rrb.len(), 1024);
    //assert_eq!(rrb.cap(), 1024);

    //// 没有了，触发扩容
    //let buf = rrb.as_mut_bytes();
    //assert_eq!(buf.len(), 1024);
    //assert_eq!(rrb.cap(), 1024 * 2);
    //assert_eq!(rrb.len(), 1024);

    //rrb.advance_read(1024);

    //rrb.advance_write(1024);
    //let buf = rrb.as_mut_bytes();
    //assert_eq!(buf.len(), 1024);
    //rrb.advance_write(1024);

    //// 等待10ms。（默认是4ms）
    //std::thread::sleep(ds::time::Duration::from_millis(10));
    //let buf = rrb.as_mut_bytes();
    //assert_eq!(buf.len(), 1024);
    //rrb.advance_write(1024);
    //let buf = rrb.as_mut_bytes();
    //assert_eq!(buf.len(), 1024);

    //// 缩容
    //assert_eq!(rrb.cap(), 4 * 1024);
    //rrb.advance_read(2 * 1024);
    ////rrb.resize(2 * 1024);
    //assert_eq!(rrb.cap(), 2 * 1024);
}

// 随机生成器，生成的内存从a-z, A-Z, 0-9 循环。
struct Reader {
    num: usize,
    source: Vec<u8>,
    offset: usize,
}
impl Reader {
    fn num(&mut self, n: usize) {
        self.num = n;
        self.offset = 0;
    }
}
impl ds::BuffRead for Reader {
    type Out = usize;
    fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out) {
        assert!(self.num > 0);
        let old = self.offset;
        while self.offset < self.num {
            let oft = self.offset % self.source.len();
            let l = b.len().min(self.num - self.offset);
            use std::ptr::copy_nonoverlapping as copy;
            unsafe { copy(self.source.as_ptr().offset(oft as isize), b.as_mut_ptr(), l) };
            self.offset += l;
        }
        (self.offset - old, self.offset)
    }
}

#[test]
fn guarded_buffer() {
    let mut reader = Reader {
        offset: 0,
        source: Vec::from("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"),
        num: 0,
    };
    use ds::GuardedBuffer;
    let mut guard = GuardedBuffer::new(128, 1024, 128);
    let empty = guard.read();
    assert_eq!(empty.len(), 0);
    reader.num(24);
    let n = guard.write(&mut reader);
    assert_eq!(n, guard.len(), "{}", guard);
    assert_eq!(n, reader.num);
    println!("guarded_buffer => 2");

    let data = guard.read();
    assert_eq!(n, data.len());
    assert_eq!(data, reader.source[0..n]);
    let len_g0 = 24;
    let g0 = guard.take(len_g0);
    assert_eq!(g0.len(), len_g0);
    assert_eq!(guard.len(), n - len_g0);
    let data = guard.read();
    assert_eq!(n - len_g0, data.len());
    //g0.read(0);
    reader.num(17);
    guard.write(&mut reader);
    let g1 = guard.take(10);
    let g2 = guard.take(3);
    let g3 = guard.take(3);
    drop(g2);
    drop(g1);
    reader.num(1);
    guard.write(&mut reader);
    drop(g3);

    drop(g0);
    guard.gc();
    println!("guarded_buffer => 10");
}
