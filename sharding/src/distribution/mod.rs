mod consistent;
mod dbrange;
mod modrange;
mod modula;
//mod padding;
mod range;
mod slotmod;
mod splitmod;

use consistent::Consistent;
pub use dbrange::DBRange;
use modrange::ModRange;
use modula::Modula;
//use padding::Padding;
pub use range::Range;
use splitmod::SplitMod;

use crate::distribution::slotmod::SlotMod;

#[derive(Clone, Debug)]
pub enum Distribute {
    //Padding(Padding),
    Consistent(Consistent),
    Modula(Modula),
    Range(Range),
    ModRange(ModRange),
    SplitMod(SplitMod),
    SlotMod(SlotMod),
}

//pub const DIST_PADDING: &str = "padding";
//pub const DIST_MODULA: &str = "modula";
//pub const DIST_ABS_MODULA: &str = "absmodula";
//pub const DIST_KETAMA: &str = "ketama";

// 默认的range分布策略是range，对应的slot是256，如果slot数是xxx(非256)，则需要设置为：range-xxx，shard 需要是2的n次方
pub const DIST_RANGE: &str = "range";
// modrange，用于mod后再分区间, shard 需要是2的n次方
pub const DIST_MOD_RANGE: &str = "modrange";

// 默认采用的slot是256，slot等价于client的hash-gen概念，即集群的槽（虚拟节点）的总数，
// 每个redis分片会保存某个范围的slot槽点，多个redis 分片(shard)就组合成一个cluster
const DIST_RANGE_SLOT_COUNT_DEFAULT: u64 = 256;

//const DIST_RANGE_WITH_SLOT_PREFIX: &str = "range-";

// modrange，用于mod后再分区间
//const DIST_MOD_RANGE_WITH_SLOT_PREFIX: &str = "modrange-";

// splitmod
//const DIST_SPLIT_MOD_WITH_SLOT_PREFIX: &str = "splitmod-";

// slotmod
//const DIST_SLOT_MOD_PREFIX: &str = "slotmod-";

impl Distribute {
    pub fn from(distribution: &str, names: &Vec<String>) -> Self {
        let dist = distribution.to_ascii_lowercase();
        let idx = dist.find('-');
        let name = &dist[..idx.unwrap_or(dist.len())];
        let num = idx.map(|i| dist[i + 1..].parse::<u64>().ok()).flatten();

        match name {
            //DIST_PADDING => Self::Padding(Default::default()),
            "modula" => Self::Modula(Modula::from(names.len(), false)),
            "absmodula" => Self::Modula(Modula::from(names.len(), true)),
            "ketama" => Self::Consistent(Consistent::from(names)),
            "range" => Self::Range(Range::from(num, names.len())),
            "modrange" => Self::ModRange(ModRange::from(num, names.len())),
            "splitmod" => Self::SplitMod(SplitMod::from(num, names.len())),
            "slotmod" => Self::SlotMod(SlotMod::from(num, names.len())),
            _ => {
                log::warn!("'{}' is not valid , use modula instead", distribution);
                Self::Modula(Modula::from(names.len(), false))
            }
        }
    }
    // 适配mysql 动态shands
    // pub fn from_num(distribution: &str, num: usize) -> Self {
    //     let dist = distribution.to_ascii_lowercase();
    //     match dist.as_str() {
    //         DIST_MODULA => Self::Modula(Modula::from(num, false)),
    //         _ => {
    //             log::warn!("'{}' is not valid , use modula instead", distribution);
    //             Self::Modula(Modula::from(num, false))
    //         }
    //     }
    // }
    #[inline]
    pub fn index(&self, hash: i64) -> usize {
        match self {
            //Self::Padding(pd) => pd.index(hash),
            Self::Consistent(d) => d.index(hash),
            Self::Modula(d) => d.index(hash),
            Self::Range(r) => r.index(hash),
            Self::ModRange(m) => m.index(hash),
            Self::SplitMod(s) => s.index(hash),
            Self::SlotMod(s) => s.index(hash),
        }
    }
}

// 默认不分片
impl Default for Distribute {
    fn default() -> Self {
        Self::Modula(Modula::from(1, false))
    }
}
