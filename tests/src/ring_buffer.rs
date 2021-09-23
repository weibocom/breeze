#[cfg(test)]
mod tests {
    use ds::RingBuffer;
    use rand::Rng;
    use std::ptr::copy_nonoverlapping;

    fn rnd_write(w: &mut [u8], size: usize) -> Vec<u8> {
        debug_assert!(size <= w.len());
        let data: Vec<u8> = (0..size).map(|_| rand::random::<u8>()).collect();
        unsafe { copy_nonoverlapping(data.as_ptr(), w.as_mut_ptr(), size) };
        data
    }

    #[test]
    fn test_ring_buffer() {
        let cap = 32;
        let mut buffer = RingBuffer::with_capacity(cap);
        let data = rnd_write(buffer.as_mut_bytes(), cap);
        let response = buffer.processing_bytes();

        let mut rrb = ds::ResizedRingBuffer::with_capacity(cap);
        rrb.resize();
        rrb.reset_read(64);
        assert_eq!(response.len(), 0);
    }
}
