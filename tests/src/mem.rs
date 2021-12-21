#[cfg(test)]
mod mem {
    use ds::{RingBuffer, RingSlice};
    use std::time::{Duration, Instant};

    fn rnd_bytes(size: usize) -> Vec<u8> {
        let data: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
        data
    }

    #[test]
    fn ring_buffer() {
        let cap = 32;
        let data = rnd_bytes(cap);
        let rs = RingSlice::from(data.as_ptr(), cap, 0, 17);
        let mut buf = RingBuffer::with_capacity(cap);
        buf.write(&rs);
        assert_eq!(buf.len(), 17);
        assert!(&(buf.data()) == &data[0..17]);
        let rs = RingSlice::from(data.as_ptr(), cap, 32, 32 + 32);
        buf.advance_read(buf.len());
        assert_eq!(buf.len(), 0);
        let n = buf.write(&rs);
        assert_eq!(buf.len(), n);
        assert_eq!(&buf.data(), &data[..]);
        buf.advance_read(rs.len());
        assert_eq!(buf.len(), 0);
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

        let mut rrb = ds::ResizedRingBuffer::from(256, 4 * 1024, 1024, |cap, delta| {
            println!("resize {} => {}", cap, delta);
        });
        assert_eq!(1024, rrb.cap());
        assert_eq!(0, rrb.len());

        // 一次写满
        let buf = rrb.as_mut_bytes();
        assert_eq!(buf.len(), 1024);
        assert_eq!(rrb.len(), 0);
        assert_eq!(rrb.cap(), 1024);
        rrb.advance_write(1024);
        assert_eq!(rrb.len(), 1024);
        assert_eq!(rrb.cap(), 1024);

        // 没有了，触发扩容
        let buf = rrb.as_mut_bytes();
        assert_eq!(buf.len(), 1024);
        assert_eq!(rrb.cap(), 1024 * 2);
        assert_eq!(rrb.len(), 1024);

        rrb.advance_read(1024);

        rrb.advance_write(1024);
        let buf = rrb.as_mut_bytes();
        assert_eq!(buf.len(), 1024);
        rrb.advance_write(1024);

        // 等待10ms。（默认是4ms）
        std::thread::sleep(std::time::Duration::from_millis(10));
        let buf = rrb.as_mut_bytes();
        assert_eq!(buf.len(), 1024);
        rrb.advance_write(1024);
        let buf = rrb.as_mut_bytes();
        assert_eq!(buf.len(), 1024);

        // 缩容
        assert_eq!(rrb.cap(), 4 * 1024);
        rrb.advance_read(2 * 1024);
        rrb.resize(2 * 1024);
        assert_eq!(rrb.cap(), 2 * 1024);
    }

    // 随机生成器，生成的内存从a-z, A-Z, 0-9 循环。
    struct Reader {
        num: usize,
        source: Vec<u8>,
        offset: usize,
    }
    impl ds::BuffRead for Reader {
        type Out = usize;
        fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out) {
            assert!(self.num > 0);
            let mut w = 0;
            while w < self.num {
                let oft = self.offset % self.source.len();
                let l = b.len().min(self.num - w);
                use std::ptr::copy_nonoverlapping as copy;
                unsafe { copy(self.source.as_ptr().offset(oft as isize), b.as_mut_ptr(), l) };
                w += l;
                self.offset += l;
            }
            (w, w)
        }
    }

    #[test]
    fn guarded_buffer() {
        // 测试MemGuard
        //let data: Vec<u8> = "abcdefg".into();
        //let s: &[u8] = &data;
        //let slice: RingSlice = s.into();
        //let g0: MemGuard = slice.into();
        //assert_eq!(g0.read(0), &data);
        //// 指向同一块内存
        //assert_eq!(g0.read(0).as_ptr() as usize, data.as_ptr() as usize);
        //g0.recall();
        //// 内存回收，共享内存被释放。数据被复制，指向不同的内存。
        //assert_ne!(g0.read(0).as_ptr() as usize, data.as_ptr() as usize);
        //// 但是数据一致
        //assert_eq!(g0.read(0), &data);
        ////assert_eq!(g0.len(), data.len());
        //drop(data);

        println!("=====================\n");

        let mut reader = Reader {
            offset: 0,
            source: Vec::from("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"),
            num: 0,
        };
        use ds::{GuardedBuffer, MemGuard};
        let mut guard = GuardedBuffer::new(128, 1024, 128, |_, _| {});
        let empty = guard.read();
        assert_eq!(empty.len(), 0);
        reader.num = 24;
        let n = guard.write(&mut reader);
        assert_eq!(n, guard.len());
        assert_eq!(n, reader.num);

        let data = guard.read();
        assert_eq!(n, data.len());
        assert_eq!(data, reader.source[0..n]);
        let len_g0 = 24;
        let g0 = guard.take(len_g0);
        assert_eq!(g0.len(), len_g0);
        assert_eq!(guard.len(), n - len_g0);
        let data = guard.read();
        assert_eq!(n - len_g0, data.len());
        g0.read(0);
        reader.num = 17;
        guard.write(&mut reader);
        let g1 = guard.take(10);
        let g2 = guard.take(3);
        let g3 = guard.take(3);
        drop(g2);
        drop(g1);
        reader.num = 1;
        guard.write(&mut reader);

        println!("buf:{}", guard);
    }
}
