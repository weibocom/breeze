use std::ptr::copy_nonoverlapping as copy;
struct BlackholeRead(usize);
impl ds::BuffRead for BlackholeRead {
    type Out = ();
    fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out) {
        let left = self.0.min(b.len());
        if left > 0 {
            unsafe { std::ptr::write_bytes(b.as_mut_ptr(), 1u8, left) };
        }
        (left, ())
    }
}
struct WithData<T: AsRef<[u8]>> {
    oft: usize,
    data: T,
}
impl<T: AsRef<[u8]>> ds::BuffRead for WithData<T> {
    type Out = ();
    fn read(&mut self, b: &mut [u8]) -> (usize, Self::Out) {
        let data = self.data.as_ref();
        let left = (data.len() - self.oft).min(b.len());
        if left > 0 {
            unsafe {
                copy(
                    data.as_ptr().offset(self.oft as isize),
                    b.as_mut_ptr(),
                    left,
                );
            }
        }
        self.oft += left;
        println!("left {}", left);
        (left, ())
    }
}
impl<T: AsRef<[u8]>> From<T> for WithData<T> {
    fn from(data: T) -> Self {
        Self { oft: 0, data }
    }
}

//#[test]
//fn test_redis_panic() {
//    const CAP: usize = 2048;
//    let start = 4636157;
//    let _end = 4637617;
//    let mut buf = GuardedBuffer::new(2048, 4096 * 1024, CAP);
//    let mut w = 0;
//    while w < start {
//        let batch = (start - w).min(511);
//        buf.write(&mut BlackholeRead(batch));
//        w += buf.len();
//        let guard = buf.take(buf.len());
//        drop(guard);
//        buf.gc();
//    }
//    println!("buf:{}", buf);
//    let mut data = WithData::from(&DATA[..]);
//    buf.write(&mut data);
//    println!("buf:{}", buf);
//    let rs = buf.slice();
//    println!("ring slice:{:?}", rs);
//    for i in 0..rs.len() {
//        assert_eq!(DATA[i], rs.at(i));
//    }
//
//    use protocol::Protocol;
//    let redis = protocol::redis::Redis {};
//    assert!(redis.parse_response(&mut buf).is_ok(), "{:?}", buf);
//}
//const DATA: [u8; 0] = [];
