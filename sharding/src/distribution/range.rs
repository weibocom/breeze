// 按区间进行分布，比如[0,16)分区，每段间隔是4，则分为4个区间：[0,4),[4,8),[8,12),[12,16)
#[derive(Clone, Debug, Default)]
pub struct Range {
    interval: u64,
}

// Range 分布方法，总范围是[0,256)，根据分片数确定每个分片中table/interval的数量，hash按table/interval数量等量分布
const RANGE_LEN: u64 = 256;
impl Range {
    pub fn from(shards: usize) -> Self {
        debug_assert!(shards > 0 && RANGE_LEN > shards as u64);
        Range {
            interval: RANGE_LEN / shards as u64,
        }
    }

    pub fn index(&self, hash: u64) -> usize {
        let rs = hash / self.interval;
        rs as usize
    }
}
