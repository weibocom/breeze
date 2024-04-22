use ds::EphemeralVec;

#[test]
fn ephemera_vec() {
    use ds::Buffer;
    let mut v = EphemeralVec::fix_cap(1024);
    v.write(b"abcdefg");
    assert_eq!(v.len(), 7);
    assert_eq!(&*v, b"abcdefg");
    v.write(b"hijklmnop");
    assert_eq!(v.len(), 16);
    assert_eq!(&*v, b"abcdefghijklmnop");

    drop(v);

    // 单线程顺序分配，立即释放
    let mut n = 0;
    // 20M数据。Cache大小是8M，可以循环分配
    use rand::{Rng, RngCore};
    let rnd = &mut rand::thread_rng();
    while n < 16 << 20 {
        let len = rnd.gen_range(1..16 * 1024);
        let mut v = EphemeralVec::fix_cap(len);
        // 随机填充
        let mut buf = vec![0; len];
        rnd.fill_bytes(&mut buf[..]);
        v.write(&buf);
        assert_eq!(v.len(), len);
        assert_eq!(&*v, &buf[..]);
        n += len;
    }
}

#[test]
#[ignore]
fn test_ephemera_vec_thread() {
    let secs = 3600;
    let threads = 4;
    let ths: Vec<_> = (0..threads)
        .map(|_| {
            std::thread::spawn(move || {
                let start = std::time::Instant::now();
                use ds::Buffer;
                use rand::{Rng, RngCore};
                let rnd = &mut rand::thread_rng();
                // 创建1..10个随机大小的vec
                while start.elapsed().as_secs() < secs {
                    let num = rnd.gen_range(1..10);
                    let mut vecs = Vec::with_capacity(num);
                    for _i in 0..num {
                        let len = rnd.gen_range(1..64 * 1024);
                        let mut v = EphemeralVec::fix_cap(len);
                        let mut buf = vec![0; len];
                        rnd.fill_bytes(&mut buf);
                        v.write(&buf);
                        assert_eq!(v.len(), len);
                        assert_eq!(&*v, &buf);
                        vecs.push(v);
                    }
                }
            })
        })
        .collect();

    for th in ths {
        th.join().expect("should not panic");
    }
}
