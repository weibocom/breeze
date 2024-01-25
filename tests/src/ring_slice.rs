use std::{mem::size_of, num::NonZeroUsize};

use bytes::BufMut;

use byteorder::{BigEndian, ByteOrder, LittleEndian};
use ds::{ByteOrder as RingSliceByteOrder, RingSlice};
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
    let (f, s) = in_range.data_r(0);
    assert_eq!(f, &dc[0..32]);
    assert!(s.len() == 0);

    // 截止到末尾的
    let end_range = RingSlice::from(ptr, cap, cap - 32, cap);
    //let s = end_range.as_slices();
    //assert_eq!(s.len(), 1);
    assert_eq!(end_range.len(), 32);
    assert_eq!(end_range, dc[cap - 32..cap]);
    let (f, s) = in_range.data_r(0);
    assert_eq!(f, &dc[0..32]);
    assert!(s.len() == 0);

    let over_range = RingSlice::from(ptr, cap, cap - 32, cap + 32);
    //let s = over_range.as_slices();
    //assert_eq!(over_range.len(), 64);
    //assert_eq!(s.len(), 2);
    assert_eq!(over_range.data(), (&dc[cap - 32..cap], &dc[0..32]));
    let mut v: Vec<u8> = Vec::new();
    over_range.copy_to_vec(&mut v);
    assert_eq!(&v[0..32], &dc[cap - 32..cap]);
    assert_eq!(&v[32..], &dc[0..32]);
    let (f, s) = over_range.data_r(0);
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
    assert_eq!(u32_num, num_range.u32_be(32));

    assert_eq!(u32_num, num_range.u32_be(23));

    // 验证查找\r\n
    let mut lines = RingSlice::from(ptr, cap, cap - 32, cap + 32);
    lines.update(9, b'\r');
    lines.update(20, b'\r');
    lines.update(21, b'\n');
    lines.update(62, b'\r');
    lines.update(63, b'\n');

    let line = lines.find_lf_cr(0);
    assert!(line.is_some(), "{lines:?}");
    let r = lines.find(0, b'\r');
    assert!(r.is_some());
    assert_eq!(line.expect("line"), 20);
    assert_eq!(r.expect("line-r"), 9);
    assert_eq!(lines.find_lf_cr(22), Some(62), "{lines:?}");

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
            // RingSliceByteOrder
            assert_eq!(BigEndian::read_u16(slice), rs.u16_be(i));
            assert_eq!(LittleEndian::read_i16(slice), rs.i16_le(i));
            assert_eq!(LittleEndian::read_u16(slice), rs.u16_le(i));

            assert_eq!(LittleEndian::read_i24(slice), rs.i24_le(i));
            assert_eq!(
                BigEndian::read_i24(slice),
                rs.i24_be(i),
                "{i} => {:?} => {rs} => {slice:?}",
                unsafe { rs.data_dump() }
            );

            assert_eq!(BigEndian::read_u32(slice), rs.u32_be(i));
            assert_eq!(LittleEndian::read_i32(slice), rs.i32_le(i));
            assert_eq!(LittleEndian::read_u32(slice), rs.u32_le(i));

            assert_eq!(LittleEndian::read_u48(slice), rs.u48_le(i));
            assert_eq!(LittleEndian::read_i48(slice), rs.i48_le(i),);

            assert_eq!(BigEndian::read_u64(slice), rs.u64_be(i));
            assert_eq!(LittleEndian::read_i64(slice), rs.i64_le(i));

            assert_eq!(BigEndian::read_u64(slice), rs.u64_be(i));
            assert_eq!(LittleEndian::read_i64(slice), rs.i64_le(i));
        }
    }
}

#[test]
fn read_number_one() {
    let v = [250, 63, 209, 177, 37, 221, 128, 235];
    let rs: RingSlice = (&v[..]).into();
    assert_eq!(rs.i24_le(0), LittleEndian::read_i24(&v));
    assert_eq!(rs.u48_le(0), LittleEndian::read_u48(&v));
    assert_eq!(rs.i48_le(0), LittleEndian::read_i48(&v));
}

#[test]
fn copy_to_vec() {
    let mut data = vec![0, 1, 2];
    let slice = RingSlice::from_vec(&data);

    slice.copy_to_vec(&mut data);
    assert_eq!(data, vec![0, 1, 2, 0, 1, 2]);
}

#[test]
fn copy_to_slice() {
    let data = vec![0, 1, 2];
    let slice = RingSlice::from_vec(&data);

    let mut slice_short = [0_u8; 2];
    slice.copy_to_w(0..2, &mut slice_short[..]);
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
                let end = start + rng.gen_range(1..cap);
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

    assert_eq!(num1, slice.u64_le(0));
    assert_eq!(num2, slice.u32_le(size_of::<u64>()));
    assert_eq!(num3, slice.u16_le(size_of::<u64>() + size_of::<u32>()));
    assert_eq!(
        num4,
        slice.u8(size_of::<u64>() + size_of::<u32>() + size_of::<u16>())
    );
    assert_eq!(
        num5,
        slice.u48_le(size_of::<u64>() + size_of::<u32>() + size_of::<u16>() + size_of::<u8>())
    );
    assert_eq!(
        num6,
        slice.u56_le(
            size_of::<u64>()
                + size_of::<u32>()
                + size_of::<u16>()
                + size_of::<u8>()
                + num5_bytes.len()
        )
    );
    assert_eq!(
        num7,
        slice.i32_le(
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

    assert_eq!(num1, slice.u64_be(0));
    assert_eq!(num2, slice.u32_be(size_of::<u64>()));
    assert_eq!(num3, slice.u16_be(size_of::<u64>() + size_of::<u32>()));
    assert_eq!(
        num4,
        slice.u8(size_of::<u64>() + size_of::<u32>() + size_of::<u16>())
    );
}

#[test]
fn data_r() {
    let cap = 512;
    let mut data: Vec<u8> = (0..cap).map(|_| rand::random::<u8>()).collect();
    let ptr = data.as_mut_ptr();
    for _ in 0..100 {
        let start = rand::thread_rng().gen_range(0..cap);
        let rs = RingSlice::from(ptr, cap, start, start + cap);
        // 随机选一个oft与len
        // 对比data_r与data的结果
        for _ in 0..64 {
            let oft = rand::thread_rng().gen_range(0..rs.len());
            let len = rand::thread_rng().gen_range(0..rs.len() - oft);
            println!("oft:{}, end:{}", oft, len + oft);
            let (first, sec) = rs.data_r(oft..oft + len);
            let (first2, sec2) = rs.data_r(oft..oft + len);
            assert_eq!(first, first2);
            assert_eq!(sec, sec2);
        }
    }
}

#[test]
fn fold() {
    let data = &b"12345678abcdefg9"[..];
    let rs: RingSlice = data.into();
    let num = rs.fold_r(0.., 0u64, |acc, v| {
        let ascii = v.is_ascii_digit();
        if ascii {
            *acc = acc.wrapping_mul(10).wrapping_add((v - b'0') as u64);
        }
        ascii
    });
    assert_eq!(num, 12345678);
    let start = data.len() - 1; // '9'
    let end = start + 9; // '8'
    let rs = RingSlice::from(data.as_ptr(), data.len(), start, end);
    let num = rs.fold_r(.., 0u64, |acc, v| {
        let ascii = v.is_ascii_digit();
        if ascii {
            *acc = acc.wrapping_mul(10).wrapping_add((v - b'0') as u64);
        }
        ascii
    });
    assert_eq!(num, 912345678);
}
