#[cfg(test)]
mod bit_map_test {
    use rand::Rng;
    use std::collections::HashMap;
    #[test]
    fn test_bit_map() {
        let cap = 256;
        let bits = ds::BitMap::with_capacity(cap);
        for i in 0..cap {
            bits.mark(i);
            assert!(bits.marked(i));
            bits.unmark(i);
            assert!(!bits.marked(i));
            bits.unmark(i);
            assert!(!bits.marked(i));
        }
        assert_eq!(bits.take().len(), 0);

        let mut marked = HashMap::with_capacity(64);
        let mut rng = rand::thread_rng();
        for _i in 0..10 {
            let pos = rng.gen::<usize>() % cap;
            marked.insert(pos, ());
            bits.mark(pos);
        }
        let mut marked = marked.iter().map(|(&pos, _)| pos).collect::<Vec<usize>>();
        marked.sort();

        assert_eq!(marked, bits.take());
    }
}
