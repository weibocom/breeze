// 算法： slot = hash % slot_count；通过slot查询所在的shard index
#[derive(Clone, Debug, Default)]
pub struct SlotMap {
    slot_count: u64,
    slot_mapping: Vec<u16>, // slot到shard的映射关系：vec[slot] = shard index
}

impl SlotMap {
    // slot_map slot区间与shard的映射关系，shard从小到大排列
    // 映射关系： [0,255]:shard0;[256,511]:shard1;[512,767]:shard2;[768,1023]:shard3
    // 配置：[0,255];[256,511];[512,767];[768,1023]
    pub fn from(shards: usize, slotmap: &str) -> Self {
        let mut slot_max = 0;
        let mut slot_mapping: Vec<u16> = Vec::with_capacity(1024);

        // 将字符串分割成各个区间
        let intervals: Vec<&str> = slotmap.split(';').collect();
        assert_eq!(shards, intervals.len());

        // 解析每个区间
        for (i, interval) in intervals.iter().enumerate() {
            // 去除方括号
            let interval = interval.trim_matches(|c| c == '[' || c == ']');

            // 将字符串分割成两个数字
            let parts: Vec<&str> = interval.split(',').collect();

            // 解析数字
            use std::str::FromStr;
            let start: u16 = u16::from_str(parts[0]).unwrap_or(0);
            let end: u16 = u16::from_str(parts[1]).unwrap_or(0);
            for _ in start..=end {
                slot_mapping.push(i as u16);
            }
            if slot_max < end {
                slot_max = end;
            }

            // debug模式下输出解析结果
            log::debug!("区间{}: {} - {}", i, start, end);
        }
        let slot_count = slot_max as u64 + 1;
        assert_eq!(slot_mapping.len(), slot_count as usize);

        SlotMap {
            slot_count: slot_count,
            slot_mapping,
        }
    }

    pub fn index(&self, hash: i64) -> usize {
        let slot = hash.wrapping_rem(self.slot_count as i64);
        self.slot_mapping[slot as usize] as usize
    }
}
