
#[cfg(test)]
mod tests_ds {
    use super::RingSlice;
    #[test]
    fn test_ring_slice() {
        let cap = 1024;
        let mut data: Vec<u8> = (0..cap).map(|_| rand::random::<u8>()).collect();
        let dc = data.clone();
        let ptr = data.as_mut_ptr();
        std::mem::forget(data);
        let mut in_range = RingSlice::from(ptr, cap, 0, 32);
        let mut buf = vec![0u8; cap];
        let n = in_range.read(&mut buf);
        assert_eq!(in_range.available(), 0);
        assert_eq!(&buf[..n], &dc[in_range.start..in_range.end]);

        // 截止到末尾的
        let mut end_range = RingSlice::from(ptr, cap, cap - 32, cap);
        let n = end_range.read(&mut buf);
        assert_eq!(end_range.available(), 0);
        assert_eq!(&buf[0..n], &dc[end_range.start..end_range.end]);

        let mut over_range = RingSlice::from(ptr, cap, cap - 32, cap + 32);
        let n = over_range.read(&mut buf);
        assert_eq!(over_range.available(), 0);
        let mut merged = (&dc[cap - 32..]).clone().to_vec();
        merged.extend(&dc[0..32]);
        assert_eq!(&buf[0..n], &merged);

        let u32_num = 111234567u32;
        let bytes = u32_num.to_be_bytes();
        unsafe {
            log::debug!("bytes:{:?}", bytes);
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
