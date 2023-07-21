use std::num::NonZeroUsize;

use byteorder::LittleEndian;
use ds::RingSlice;
use rand::Rng;
#[test]
fn test_ring_slice() {
    let cap = 1024;
    let mut data: Vec<u8> = (0..cap)
        .map(|_| rand::random::<u8>().max(b'a').min(b'z'))
        .collect();
    let dc = data.clone();
    let ptr = data.as_mut_ptr();
    std::mem::forget(data);

    let in_range = RingSlice::from(ptr, cap, 0, 32);
    assert_eq!(in_range.len(), 32);
    assert_eq!(in_range, dc[0..32]);
    let (f, s) = in_range.data_oft(0);
    assert_eq!(f, &dc[0..32]);
    assert!(s.len() == 0);

    // 截止到末尾的
    let end_range = RingSlice::from(ptr, cap, cap - 32, cap);
    //let s = end_range.as_slices();
    //assert_eq!(s.len(), 1);
    assert_eq!(end_range.len(), 32);
    assert_eq!(end_range, dc[cap - 32..cap]);
    let (f, s) = in_range.data_oft(0);
    assert_eq!(f, &dc[0..32]);
    assert!(s.len() == 0);

    let over_range = RingSlice::from(ptr, cap, cap - 32, cap + 32);
    //let s = over_range.as_slices();
    //assert_eq!(over_range.len(), 64);
    //assert_eq!(s.len(), 2);
    assert_eq!(over_range, (&dc[cap - 32..cap], &dc[0..32]));
    let mut v: Vec<u8> = Vec::new();
    over_range.copy_to_vec(&mut v);
    assert_eq!(&v[0..32], &dc[cap - 32..cap]);
    assert_eq!(&v[32..], &dc[0..32]);
    let (f, s) = over_range.data_oft(0);
    assert_eq!(f, &dc[cap - 32..cap]);
    assert_eq!(s, &dc[0..32]);

    let u32_num = 111234567u32;
    let bytes = u32_num.to_be_bytes();
    unsafe {
        //log::debug!("bytes:{:?}", bytes);
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.offset(8), 4);
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.offset(1023), 1);
        std::ptr::copy_nonoverlapping(bytes.as_ptr().offset(1), ptr, 3);
    }
    let num_range = RingSlice::from(ptr, cap, 1000, 1064);
    assert_eq!(u32_num, num_range.read_u32(32));

    assert_eq!(u32_num, num_range.read_u32(23));

    // 验证查找\r\n
    let mut lines = RingSlice::from(ptr, cap, cap - 32, cap + 32);
    lines.update(9, b'\r');
    lines.update(20, b'\r');
    lines.update(21, b'\n');
    lines.update(62, b'\r');
    lines.update(63, b'\n');

    let line = lines.find_lf_cr(0);
    let r = lines.find(0, b'\r');
    assert!(line.is_some());
    assert!(r.is_some());
    assert_eq!(line.expect("line"), 20);
    assert_eq!(r.expect("line-r"), 9);
    assert_eq!(lines.find_lf_cr(22).unwrap(), 62);

    let _ = unsafe { Vec::from_raw_parts(ptr, 0, cap) };
}

#[test]
fn test_read_number() {
    let cap = 1024;
    let mut data: Vec<u8> = (0..cap)
        .map(|_| rand::random::<u8>().max(b'a').min(b'z'))
        .collect();
    let ptr = data.as_mut_ptr();
    std::mem::forget(data);
    // 运行1000次，随机写入一个数
    for _ in 0..1000 {
        let start = rand::random::<u32>() as usize;
        let len = (rand::random::<usize>() % cap).max(16);
        let end = start + len;
        let rs = RingSlice::from(ptr, cap, start, end);
        let oft: usize = rand::thread_rng().gen_range(0..len - 8);
        assert_eq!(rs.read_u64(oft), rs.read_num_be(oft));
    }
    let _ = unsafe { Vec::from_raw_parts(ptr, 0, cap) };
}

#[test]
fn copy_to_vec() {
    let mut data = vec![0, 1, 2];
    let slice = RingSlice::from_vec(&data);

    slice.copy_to_vec(&mut data);
    assert_eq!(data, vec![0, 1, 2, 0, 1, 2]);
    println!("new data:{:?}", data);
}

#[test]
fn copy_to_slice() {
    let data = vec![0, 1, 2];
    let slice = RingSlice::from_vec(&data);

    let mut slice_short = [0_u8; 2];
    slice.copy_to_slice(&mut slice_short);
    assert_eq!(slice_short, [0, 1]);

    let mut slice_long = [0_u8; 6];
    slice.copy_to_slice(&mut slice_long[3..6]);
    assert_eq!(slice_long, [0, 0, 0, 0, 1, 2]);
}

use byteorder::ByteOrder;
#[test]
fn check_header() {
    let header = [1, 0, 0, 1, 1];
    let len = LittleEndian::read_u24(&header) as usize;
    println!("header len: {}", len);

    match NonZeroUsize::new(len) {
        Some(_chunk_len) => {
            println!("ok!")
        }
        None => {
            println!("malformed len: {}", len);
            assert!(false);
        }
    };
}
