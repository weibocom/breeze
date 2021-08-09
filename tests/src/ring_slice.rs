#[cfg(test)]
mod tests_ds {
    use ds::RingSlice;
    #[test]
    fn test_ring_slice() {
        let cap = 1024;
        let mut data: Vec<u8> = (0..cap).map(|_| rand::random::<u8>()).collect();
        let dc = data.clone();
        let ptr = data.as_mut_ptr();
        std::mem::forget(data);

        let mut in_range = RingSlice::from(ptr, cap, 0, 32);
        let s = in_range.take_slice();
        assert_eq!(in_range.available(), 0);
        assert_eq!(s.data(), &dc[0..32]);

        // 截止到末尾的
        let mut end_range = RingSlice::from(ptr, cap, cap - 32, cap);
        let s = end_range.take_slice();
        assert_eq!(end_range.available(), 0);
        assert_eq!(s.data(), &dc[cap - 32..cap]);

        let mut over_range = RingSlice::from(ptr, cap, cap - 32, cap + 32);
        let s1 = over_range.take_slice();
        let s2 = over_range.take_slice();
        assert_eq!(over_range.available(), 0);
        assert_eq!(s1.data(), &dc[cap - 32..cap]);
        assert_eq!(s2.data(), &dc[0..32]);

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

        let _ = unsafe { Vec::from_raw_parts(ptr, 0, cap) };
    }
}
