use std::{mem::size_of, num::NonZeroUsize};

use bytes::BufMut;

use byteorder::{BigEndian, ByteOrder, LittleEndian};
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
    assert_eq!(u32_num, num_range.read_u32_be(32));

    assert_eq!(u32_num, num_range.read_u32_be(23));

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
    let cap = 128;
    let mut data: Vec<u8> = (0..cap).map(|_| rand::random::<u8>()).collect();
    let ptr = data.as_mut_ptr();
    for _ in 0..100 {
        let start = rand::thread_rng().gen_range(0..cap);
        let rs = RingSlice::from(ptr, cap, start, start + cap);
        let mut c = Vec::with_capacity(cap);
        c.extend_from_slice(&data[start..]);
        c.extend_from_slice(&data[..start]);

        for i in 0..cap - 8 {
            let slice = &c[i..];
            assert_eq!(BigEndian::read_u16(slice), rs.read_u16_be(i));
            assert_eq!(LittleEndian::read_i16(slice), rs.read_i16_le(i));
            assert_eq!(BigEndian::read_u32(slice), rs.read_u32_be(i));
            assert_eq!(LittleEndian::read_i32(slice), rs.read_i32_le(i));
            assert_eq!(BigEndian::read_u64(slice), rs.read_u64_be(i));
            assert_eq!(LittleEndian::read_i64(slice), rs.read_i64_le(i));
            assert_eq!(LittleEndian::read_i24(slice), rs.read_i24_le(i));
            assert_eq!(LittleEndian::read_u48(slice), rs.read_u48_le(i));

            assert_eq!(LittleEndian::read_i24(slice), rs.read_i24_le_cmp(i));
            assert_eq!(LittleEndian::read_u48(slice), rs.read_u48_le_cmp(i));

            assert_eq!(BigEndian::read_i24(slice), rs.read_i24_be_cmp(i),);
            assert_eq!(BigEndian::read_u48(slice), rs.read_u48_be_cmp(i));

            assert_eq!(BigEndian::read_u16(slice), rs.read_u16_be_cmp(i));
            assert_eq!(LittleEndian::read_i16(slice), rs.read_i16_le_cmp(i));
            assert_eq!(BigEndian::read_u32(slice), rs.read_u32_be_cmp(i));
            assert_eq!(LittleEndian::read_i32(slice), rs.read_i32_le_cmp(i));
            assert_eq!(BigEndian::read_u64(slice), rs.read_u64_be_cmp(i));
            assert_eq!(LittleEndian::read_i64(slice), rs.read_i64_le_cmp(i));
        }
    }
}

#[test]
fn read_number_one() {
    let v = vec![
        250, 63, 209, 177, 238, 67, 85, 116, 95, 81, 12, 62, 104, 150, 17, 43, 119, 187, 244, 129,
        17, 7, 205, 211, 229, 132, 223, 237, 172, 21, 157, 168, 78, 37, 10, 84, 195, 177, 70, 98,
        201, 244, 157, 98, 105, 69, 32, 80, 149, 122, 2, 89, 138, 133, 219, 72, 67, 248, 86, 146,
        233, 124, 31, 162, 137, 56, 81, 59, 11, 160, 158, 51, 226, 200, 242, 14, 36, 254, 39, 243,
        27, 168, 67, 184, 100, 175, 209, 131, 217, 229, 175, 66, 191, 74, 61, 72, 183, 36, 98, 68,
        240, 42, 77, 225, 67, 208, 203, 151, 240, 154, 105, 127, 237, 27, 10, 213, 48, 54, 13, 22,
        69, 171, 0, 223, 68, 219, 84, 149,
    ];
    let rs = RingSlice::from_vec(&v);
    assert_eq!(rs.read_i24_le_cmp(0), LittleEndian::read_i24(&v));
    assert_eq!(rs.read_i24_be_cmp(0), BigEndian::read_i24(&v));
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

    let cap = 1024;
    let mask = cap - 1;
    let raw: Vec<u8> = (0..cap).map(|_| rand::random::<u8>()).collect();
    let ptr = raw.as_ptr();
    let mut rng = rand::thread_rng();
    let mut dst = Vec::with_capacity(cap);
    unsafe { dst.set_len(cap) };
    for _i in 0..100 {
        let (start, end) = match rng.gen_range(0..10) {
            0 => (0, cap),
            1 => (cap, cap * 2),
            2 => {
                let start = rng.gen::<usize>() & mask;
                (start, start + cap)
            }
            _ => {
                let start = rng.gen::<usize>() & mask;
                let end = start + rng.gen_range(0..cap);
                (start, end)
            }
        };
        let rs = RingSlice::from(ptr, cap, start, end);
        let mut slice = Vec::with_capacity(end - start);
        // 把从start..end的内容复制到slice中
        if end <= cap {
            slice.extend_from_slice(&raw[start..end]);
        } else {
            slice.extend_from_slice(&raw[start..cap]);
            let left = end - cap;
            slice.extend_from_slice(&raw[0..left]);
        }

        // 验证64次
        for _ in 0..64 {
            // 随机选一个oft与len
            let (r_start, r_len) = match rng.gen_bool(0.5) {
                true => (0, rs.len()),
                false => {
                    let r_start = rng.gen_range(0..rs.len());
                    let r_len = rng.gen_range(0..rs.len() - r_start);
                    (r_start, r_len)
                }
            };
            rs.copy_to_cmp(&mut dst[..], r_start, r_len);
            assert_eq!(&dst[0..r_len], &slice[r_start..r_start + r_len]);
            dst.fill(0);
            rs.copy_to_r(&mut dst, r_start..r_start + r_len);
            assert_eq!(&dst[0..r_len], &slice[r_start..r_start + r_len]);
        }
    }
}

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

#[test]
fn check_read_num_le() {
    let mut data = Vec::with_capacity(1024);
    let num1 = 123456789012345_u64;
    let num2 = 12345678_u32;
    let num3 = 12345_u16;
    let num4 = 129_u8;
    let num5 = 6618611909121;
    let num5_bytes = [1, 2, 3, 4, 5, 6];
    let num6 = 1976943448883713;
    let num6_bytes = [1, 2, 3, 4, 5, 6, 7];
    let num7 = 12345678_i32;

    data.put_u64_le(num1);
    data.put_u32_le(num2);
    data.put_u16_le(num3);
    data.put_u8(num4);
    data.extend(num5_bytes);
    data.extend(num6_bytes);
    data.put_i32_le(num7);

    let slice = RingSlice::from_vec(&data);

    assert_eq!(num1, slice.read_u64_le(0));
    assert_eq!(num2, slice.read_u32_le(size_of::<u64>()));
    assert_eq!(num3, slice.read_u16_le(size_of::<u64>() + size_of::<u32>()));
    assert_eq!(
        num4,
        slice.read_u8(size_of::<u64>() + size_of::<u32>() + size_of::<u16>())
    );
    assert_eq!(
        num5,
        slice.read_u48_le(size_of::<u64>() + size_of::<u32>() + size_of::<u16>() + size_of::<u8>())
    );
    assert_eq!(
        num6,
        slice.read_u56_le(
            size_of::<u64>()
                + size_of::<u32>()
                + size_of::<u16>()
                + size_of::<u8>()
                + num5_bytes.len()
        )
    );
    assert_eq!(
        num7,
        slice.read_i32_le(
            size_of::<u64>()
                + size_of::<u32>()
                + size_of::<u16>()
                + size_of::<u8>()
                + num5_bytes.len()
                + num6_bytes.len()
        )
    );
}

#[test]
fn check_read_num_be() {
    let mut data = Vec::with_capacity(1024);
    let num1 = 123456789012345_u64;
    let num2 = 12345678_u32;
    let num3 = 12345_u16;
    let num4 = 129_u8;

    data.put_u64(num1);
    data.put_u32(num2);
    data.put_u16(num3);
    data.put_u8(num4);

    let slice = RingSlice::from_vec(&data);

    assert_eq!(num1, slice.read_u64_be(0));
    assert_eq!(num2, slice.read_u32_be(size_of::<u64>()));
    assert_eq!(num3, slice.read_u16_be(size_of::<u64>() + size_of::<u32>()));
    assert_eq!(
        num4,
        slice.read_u8(size_of::<u64>() + size_of::<u32>() + size_of::<u16>())
    );
}
