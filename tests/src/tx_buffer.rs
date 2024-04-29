use rt::TxBuffer;
#[test]
fn check_tx_buffer() {
    const MIN: usize = 4 * 1024;
    let mut buf = TxBuffer::new();
    const EMPTY: [u8; 0] = [];
    assert_eq!(buf.data(), &EMPTY[..]);
    assert_eq!(buf.cap(), 0);
    assert_eq!(buf.len(), 0);
    let v1 = b"abcdefg";
    let v2 = b"hijklmn";
    buf.write(v1);
    assert_eq!(buf.len(), v1.len());
    assert_eq!(buf.cap(), MIN);
    let data = buf.data();
    assert_eq!(data, v1);
    buf.take(1);
    assert_eq!(buf.data(), &v1[1..]);
    buf.write(v2);
    assert_eq!(buf.len(), v1.len() + v2.len());
    let mut v = Vec::with_capacity(v1.len() + v2.len());
    v.extend_from_slice(v1);
    v.extend_from_slice(v2);
    assert_eq!(buf.data(), &v[1..]);
    buf.take(v.len() - 1);
    assert_eq!(buf.len(), 0);

    // 写入一个大于4K的。
    let v = vec![b'z'; 4100];
    buf.write(&v);
    assert_eq!(buf.len(), v.len());
    assert_eq!(buf.data(), &v);
    assert_eq!(buf.cap(), MIN * 2);
    buf.take(v.len());
    assert_eq!(buf.len(), 0);
}
