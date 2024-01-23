use std::ops::Div;

#[derive(Debug, Clone)]
pub enum DBRange {
    VectorRange(VectorRange),
    UserRange(UserRange),
}

impl Default for DBRange {
    fn default() -> Self {
        Self::VectorRange(Default::default())
    }
}

impl DBRange {
    pub fn new(db_count: usize, table_count: usize, shards: usize) -> Self {
        Self::VectorRange(VectorRange {
            // db_count,
            // shards,
            table_count,
            slot: db_count * table_count,
            db_per_shard: db_count.div(shards),
        })
    }
    pub fn new_user(db_count: usize, table_count: usize) -> Self {
        Self::UserRange(UserRange {
            db_count,
            table_count,
        })
    }
    // alg: hash_abs / self.slot % self.slot
    pub fn index(&self, hash: i64) -> usize {
        match self {
            DBRange::VectorRange(inner) => inner.index(hash),
            DBRange::UserRange(inner) => inner.index(hash),
        }
    }

    // alg: hash_abs / self.db_count % self.db_count
    pub fn db_idx(&self, hash: i64) -> usize {
        match self {
            DBRange::VectorRange(inner) => inner.db_idx(hash),
            DBRange::UserRange(inner) => inner.db_idx(hash),
        }
    }

    // alg: hash_abs / self.table_count % self.table_count
    pub fn table_idx(&self, hash: i64) -> usize {
        match self {
            DBRange::VectorRange(inner) => inner.db_idx(hash),
            DBRange::UserRange(inner) => inner.db_idx(hash),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct VectorRange {
    // db_count: usize,    // db数量
    // shards: usize,      // server实际分片数
    table_count: usize,  // 每个db的table数量
    slot: usize,         // table总数: db_count * table_count
    db_per_shard: usize, // 每个shard的db数
}

impl VectorRange {
    pub fn new(db_count: usize, table_count: usize, shards: usize) -> Self {
        Self {
            // db_count,
            // shards,
            table_count,
            slot: db_count * table_count,
            db_per_shard: db_count.div(shards),
        }
    }

    // alg: hash_abs / self.slot % self.slot / self.table_count / self.db_per_shard
    pub fn index(&self, hash: i64) -> usize {
        let hash_abs = if hash > 0 {
            hash as usize
        } else {
            hash.abs() as usize
        };
        hash_abs
            .wrapping_div(self.slot)
            .wrapping_rem(self.slot)
            .wrapping_div(self.table_count)
            .wrapping_div(self.db_per_shard)
    }

    // alg: hash_abs / self.slot % self.slot / self.table_count
    pub fn db_idx(&self, hash: i64) -> usize {
        let hash_abs = if hash > 0 {
            hash as usize
        } else {
            hash.abs() as usize
        };

        hash_abs
            .wrapping_div(self.slot)
            .wrapping_rem(self.slot)
            .wrapping_div(self.table_count)
    }

    // alg: hash_abs / self.slot % self.slot % self.table_count
    pub fn table_idx(&self, hash: i64) -> usize {
        let hash_abs = if hash > 0 {
            hash as usize
        } else {
            hash.abs() as usize
        };

        hash_abs
            .wrapping_div(self.slot)
            .wrapping_rem(self.slot)
            .wrapping_rem(self.table_count)
    }
}

#[derive(Debug, Clone, Default)]
pub struct UserRange {
    db_count: usize,    // db数量
    table_count: usize, // 每个db的table数量
}

impl UserRange {
    pub fn new(db_count: usize, table_count: usize) -> Self {
        Self {
            db_count,
            table_count,
        }
    }

    // alg: hash_abs / self.slot % self.slot
    pub fn index(&self, hash: i64) -> usize {
        let hash_abs = if hash > 0 {
            hash as usize
        } else {
            hash.abs() as usize
        };

        if self.db_count == 1 {
            return 0;
        }
        hash_abs
            .wrapping_div(self.db_count)
            .wrapping_rem(self.db_count)
    }

    // alg: hash_abs / self.db_count % self.db_count
    pub fn db_idx(&self, hash: i64) -> usize {
        let hash_abs = if hash > 0 {
            hash as usize
        } else {
            hash.abs() as usize
        };

        hash_abs
            .wrapping_div(self.db_count)
            .wrapping_rem(self.db_count)
    }

    // alg: hash_abs / self.table_count % self.table_count
    pub fn table_idx(&self, hash: i64) -> usize {
        let hash_abs = if hash > 0 {
            hash as usize
        } else {
            hash.abs() as usize
        };

        hash_abs
            .wrapping_div(self.table_count)
            .wrapping_rem(self.table_count)
    }
}
