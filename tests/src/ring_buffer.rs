#[cfg(test)]
mod tests {
    //use ds::RingBuffer;
    //use rand::Rng;
    //use std::ptr::copy_nonoverlapping;
    use std::time::{Duration, Instant};

    //fn rnd_write(w: &mut [u8], size: usize) -> Vec<u8> {
    //    debug_assert!(size <= w.len());
    //    let data: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
    //    unsafe { copy_nonoverlapping(data.as_ptr(), w.as_mut_ptr(), size) };
    //    data
    //}

    #[test]
    fn test_ring_buffer() {
        //let cap = 32;
        //let mut buffer = RingBuffer::with_capacity(cap);
        //let data = rnd_write(buffer.as_mut_bytes(), cap);
        //let response = buffer.processing_bytes();

        let mut rrb = ds::ResizedRingBuffer::from(256, 4 * 1024, 1024);
        rrb.set_on_resize(|cap, delta| println!("resize {} => {}", cap, delta));
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

        rrb.advance_processed(1024);
        rrb.reset_read(1024);

        rrb.advance_processed(512);
        rrb.advance_write(1024);
        let buf = rrb.as_mut_bytes();
        assert_eq!(buf.len(), 1024);
        rrb.advance_write(1024);
        // 写满了，不会触发立即扩容
        let buf = rrb.as_mut_bytes();
        assert_eq!(buf.len(), 0);

        // 等待10ms。（默认是4ms）
        std::thread::sleep(std::time::Duration::from_millis(10));
        let buf = rrb.as_mut_bytes();
        assert_eq!(buf.len(), 1024);
        rrb.advance_write(1024);
        let buf = rrb.as_mut_bytes();
        assert_eq!(buf.len(), 1024);

        // 缩容
        rrb.advance_processed(2 * 1024);
        rrb.reset_read(3 * 1024);
        let ins = Instant::now();
        loop {
            rrb.advance_write(0);
            std::thread::sleep(std::time::Duration::from_millis(3));
            if ins.elapsed() >= Duration::from_secs(70) {
                break;
            }
        }
        rrb.advance_processed(512);
        rrb.reset_read(4 * 1024);
        println!("buffer:{}", rrb);
    }
}
