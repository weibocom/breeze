#[cfg(test)]
mod offset_tests {
    use ds::SeqOffset;
    use rand::prelude::*;
    use std::collections::HashMap;

    #[test]
    fn test_offset() {
        //test_seq_offset_one(1, 128, 32);
        //test_seq_offset_one(8, 1024, 32);
        test_seq_offset_one(64, 1024, 32);
    }

    // 一共生成num个offset，每interval次insert检查一次
    fn test_seq_offset_one(cap: usize, num: usize, interval: usize) {
        let offset = SeqOffset::with_capacity(cap);
        let seqs = gen(num);
        let mut cmp = 0;
        let mut cached = HashMap::with_capacity(cap * 2);

        let mut rng = rand::thread_rng();

        for (i, &(start, end)) in seqs.iter().enumerate() {
            let id = rng.next_u32() as usize % cap;
            offset.insert(id, start, end);
            cached.insert(start, end);

            if i % interval == 0 {
                while let Some(end) = cached.remove(&cmp) {
                    cmp = end;
                }
                assert_eq!(cmp, offset.load());
            }
        }
        while let Some(end) = cached.remove(&cmp) {
            cmp = end;
        }
        assert_eq!(cmp, offset.load());
        assert_eq!(cached.len(), 0);
    }
    // 动态生成num个 (start, end)对
    fn gen(num: usize) -> Vec<(usize, usize)> {
        let max_len = 1024 * 1024u32;
        let mut rng = rand::thread_rng();
        let nums: Vec<usize> = (0..num)
            .map(|_| 1.max(rng.next_u32() & (max_len - 1)) as usize)
            .collect();
        let mut offsets = Vec::with_capacity(num);
        let mut offset = 0;
        for len in nums[0..nums.len() - 1].iter() {
            offsets.push((offset, offset + len));
            offset += len;
        }

        let splits = 3;
        for i in 0..splits {
            let start = i * num / splits;
            let end = (start + num / splits).min(num);
            let slice = &mut offsets[start..end];
            slice.shuffle(&mut rng);
        }
        // 分为三段，三段内部分别打乱顺序
        offsets
    }
}
