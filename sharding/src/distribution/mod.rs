mod consistent;
mod modula;
mod range;

use consistent::Consistent;
use modula::Modula;
use range::Range;

#[derive(Clone, Debug)]
pub enum Distribute {
    Consistent(Consistent),
    Modula(Modula),
    Range(Range),
}

// 默认采用的slot是256，slot等价于client的hash-gen概念，即集群的槽（虚拟节点）的总数，
// 每个redis分片会保存某个范围的slot槽点，多个redis 分片(shard)就组合成一个cluster
const DIST_RANGE_SLOT_COUNT_DEFAULT: u64 = 256;
// 默认的range分布策略是range，对应的slot是256，如果slot数是xxx(非256)，则需要设置为：range-xxx
const DIST_RANGE: &str = "range";
const DIST_RANGE_WITH_SLOT_PREFIX: &str = "range-";

impl Distribute {
    pub fn from(distribution: &str, names: &Vec<String>) -> Self {
        let dist = distribution.to_ascii_lowercase();

        match dist.as_str() {
            "modula" => Self::Modula(Modula::from(names.len())),
            "ketama" => Self::Consistent(Consistent::from(names)),
            // "range" => Self::Range(Range::from(names.len())),
            _ => {
                if distribution.starts_with(DIST_RANGE) {
                    return Self::Range(Range::from(distribution, names.len()));
                }
                log::warn!("'{}' is not valid , use modula instead", distribution);
                Self::Modula(Modula::from(names.len()))
            }
        }
    }
    #[inline(always)]
    pub fn index(&self, hash: i64) -> usize {
        match self {
            Self::Consistent(d) => d.index(hash),
            Self::Modula(d) => d.index(hash),
            Self::Range(r) => r.index(hash),
        }
    }
}

// 默认不分片
impl Default for Distribute {
    fn default() -> Self {
        Self::Modula(Modula::from(1))
    }
}
