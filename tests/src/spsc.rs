#[cfg(test)]
mod tests {
    use ds::RingBuffer;
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
