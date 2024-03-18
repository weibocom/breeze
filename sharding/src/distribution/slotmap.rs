// 算法： hash % slot_count % shard_count
#[derive(Clone, Debug, Default)]
pub struct SlotMap {
    slot_count: u64,
    slot_mapping: Vec<u16>,
}

impl SlotMap {
    // slot_map slot区间与shard的映射关系，shard从小到大排列，slot_map必须有值:
    // 映射关系： [0,255]:shard0;[256,511]:shard1;[512,767]:shard2;[768,1023]:shard3
    // 配置：[0,255];[256,511];[512,767];[768,1023]
    pub fn from(num: Option<u64>, shards: usize, slot_map: Option<&str>) -> Self {
        let slot = num.unwrap_or(1024);
        use std::str::FromStr;

        let mut slot_mapping: Vec<u16> = Vec::with_capacity(slot as usize);

        // 将字符串分割成各个区间
        let intervals: Vec<&str> = slot_map.unwrap().split(';').collect();
        assert_eq!(shards, intervals.len());

        // 解析每个区间
        for (i, interval) in intervals.iter().enumerate() {
            // 去除方括号
            let interval = interval.trim_matches(|c| c == '[' || c == ']');

            // 将字符串分割成两个数字
            let parts: Vec<&str> = interval.split(',').collect();

            // 解析数字
            let start: u16 = u16::from_str(parts[0]).unwrap();
            let end: u16 = u16::from_str(parts[1]).unwrap();
            for _ in start..=end {
                slot_mapping.push(i as u16);
            }

            // 打印结果
            log::debug!("区间{}: {} - {}", i, start, end);
        }
        assert_eq!(slot_mapping.len(), slot as usize);

        SlotMap {
            slot_count: slot,
            slot_mapping,
        }
    }

    pub fn index(&self, hash: i64) -> usize {
        let idx = hash.wrapping_rem(self.slot_count as i64);
        self.slot_mapping[idx as usize] as usize
    }
}
