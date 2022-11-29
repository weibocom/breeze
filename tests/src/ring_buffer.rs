use ds::{RingBuffer, RingSlice};

fn rnd_bytes(size: usize) -> Vec<u8> {
    let data: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
    data
}

#[test]
fn test_ring_buffer() {
    let cap = 32;
    let data = rnd_bytes(cap);
    let rs = RingSlice::from(data.as_ptr(), cap, 0, 17);
    let mut buf = RingBuffer::with_capacity(cap);
    buf.write(&rs);
    assert_eq!(buf.len(), 17);
    assert!(&(buf.data()) == &data[0..17]);
    let rs = RingSlice::from(data.as_ptr(), cap, 32, 32 + 32);
    buf.advance_read(buf.len());
    let _n = buf.write(&rs);
    assert_eq!(&buf.data(), &data[..]);
    buf.advance_read(rs.len());
    // 有折返的
    let rs = RingSlice::from(data.as_ptr(), cap, 27, 27 + 19);
    buf.write(&rs);
    assert_eq!(buf.len(), rs.len());
    assert_eq!(buf.len(), 19);
    assert_eq!(buf.data(), rs);
    let rs = RingSlice::from(data.as_ptr(), cap, 32, 32 + 32);
    let n = buf.write(&rs);
    assert_eq!(n, 32 - 19);

    let mut rrb = ds::ResizedRingBuffer::from(256, 4 * 1024, 1024);
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
    let buf = rrb.as_mut_bytes();
    assert_eq!(buf.len(), 1024);
    assert_eq!(rrb.cap(), 4096);

    // 等待10ms。（默认是4ms）
    //std::thread::sleep(ds::time::Duration::from_millis(10));
    //let buf = rrb.as_mut_bytes();
    //assert_eq!(buf.len(), 1024);
    //rrb.advance_write(1024);
    //let buf = rrb.as_mut_bytes();
    //assert_eq!(buf.len(), 1024);

    // 缩容
    //rrb.advance_read(1024);
    //let ins = Instant::now();
    //loop {
    //    rrb.advance_write(0);
    //    std::thread::sleep(ds::time::Duration::from_millis(3));
    //    if ins.elapsed() >= Duration::from_secs(70) {
    //        break;
    //    }
    //}
    //rrb.advance_read(4 * 1024);
    //println!("buffer:{}", rrb);
}
