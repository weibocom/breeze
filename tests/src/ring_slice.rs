#[cfg(test)]
mod tests_ds {
    use ds::{RingSlice, Slice};
    use std::collections::HashMap;
    #[test]
    fn test_ring_slice() {
        let cap = 1024;
        let mut data: Vec<u8> = (0..cap).map(|_| rand::random::<u8>()).collect();
        let dc = data.clone();
        let ptr = data.as_mut_ptr();
        std::mem::forget(data);

        let in_range = RingSlice::from(ptr, cap, 0, 32);
        assert_eq!(in_range.len(), 32);
        assert_eq!(in_range.read(0), &dc[0..32]);

        // 截止到末尾的
        let end_range = RingSlice::from(ptr, cap, cap - 32, cap);
        //let s = end_range.as_slices();
        //assert_eq!(s.len(), 1);
        assert_eq!(end_range.len(), 32);
        assert_eq!(end_range.read(0), &dc[cap - 32..cap]);

        let over_range = RingSlice::from(ptr, cap, cap - 32, cap + 32);
        //let s = over_range.as_slices();
        //assert_eq!(over_range.len(), 64);
        //assert_eq!(s.len(), 2);
        assert_eq!(over_range.read(0), &dc[cap - 32..cap]);
        assert_eq!(over_range.read(32), &dc[0..32]);
        let mut v: Vec<u8> = Vec::new();
        over_range.copy_to_vec(&mut v);
        assert_eq!(&v[0..32], &dc[cap - 32..cap]);
        assert_eq!(&v[32..], &dc[0..32]);

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
        assert!(line.is_some());
        assert_eq!(line.expect("line"), 20);
        assert_eq!(lines.find_lf_cr(22).unwrap(), 62);

        let _ = unsafe { Vec::from_raw_parts(ptr, 0, cap) };
    }
    //#[test]
    //fn test_ring_slice_map() {
    //    let slice_data = [98u8, 114, 101, 101, 122, 101, 45, 107, 101, 121, 45, 56, 57];
    //    let slice = Slice::from(&slice_data);

    //    // 第一个字节在最后
    //    let ring_slice_data: [u8; 16] = [
    //        114, 101, 101, 122, 101, 45, 107, 101, 121, 45, 56, 57, 0, 0, 0, 98,
    //    ];
    //    let ring_slice = RingSlice::from(ring_slice_data.as_ptr(), 16, 15, 15 + slice_data.len());

    //    assert_eq!(ring_slice, slice);
    //    let slice_to_ring: RingSlice = slice.clone().into();
    //    assert_eq!(ring_slice, slice_to_ring);

    //    //assert_eq!(hash(&slice_to_ring), hash(&ring_slice));

    //    let mut m = HashMap::with_capacity(4);
    //    m.insert(ring_slice, ());
    //    assert!(m.contains_key(&slice.into()));
    //}
    //use std::collections::hash_map::DefaultHasher;
    //use std::hash::{Hash, Hasher};
    //fn hash<T: Hash>(t: &T) -> u64 {
    //    let mut s = DefaultHasher::new();
    //    t.hash(&mut s);
    //    let h = s.finish();
    //    println!("hash ring:{}", h);
    //    h
    //}

    #[test]
    fn test_split_ring_slice() {
        println!("begin");

        let data = "STORED\r\n";
        let slice = RingSlice::from(data.as_ptr(), data.len(), 0, data.len());
        let index = slice.find_sub(0, "sdfsfdssssd".as_ref());
        println!("found = {}", index.is_some());

        /*
        let data = "END\r\nVALUE key1 0 9\r\nssksksksk\r\nVALUE key2 0 13\r\nabababababaab\r\n";
        let slice = RingSlice::from(data.as_ptr(), data.len(), 21, data.len() + 21);

        //let data = "VALUE key1 0 9\r\nssksksksk\r\nVALUE key2 0 13\r\nabababababaab\r\nEND\r\n";
        //let slice = RingSlice::from(data.as_ptr(), data.len(), 0, data.len());

        //let data = "VALUE key1 0 9\r\nssksksksk\r\nVALUE key2 0 13\r\nabababababaab\r\nEND";
        //let slice = RingSlice::from(data.as_ptr(), 64, 0, data.len());
        println!("slice generated");
        let index = slice.find_sub(0, "\r\n".as_ref());
        println!("found, index = {}", index.unwrap());
        let split = slice.split("\r\n".as_ref());
        println!("slice split, size = {}", split.len());
        for single in split {
            let mut single_vec: Vec<u8> = vec![];
            single.copy_to_vec(&mut single_vec);
            println!("single = {}", String::from_utf8(single_vec).unwrap());
        }
         */
    }
}
