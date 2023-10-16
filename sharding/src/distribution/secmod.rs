/// 二阶modula，算法： hash / shards % shards
#[derive(Clone, Debug, Default)]
pub struct SecMod {
    shard_count: usize,
}

impl SecMod {
    pub fn from(shards: usize) -> Self {
        assert!(shards > 0);
        Self {
            shard_count: shards,
        }
    }

    pub fn index(&self, hash: i64) -> usize {
        // 理论上，使用secmod的业务，hash不应该是负数; 说人话就是：对负数hash，具体idx不保证确定结果
        if hash < 0 {
            log::error!("found negative hash for secmod:{}", hash);
        }
        let idx = (hash as usize)
            .wrapping_div(self.shard_count)
            .wrapping_rem(self.shard_count);

        idx
    }
}
