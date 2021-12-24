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

impl Distribute {
    pub fn from(distribution: &str, names: &Vec<String>) -> Self {
        let dist = distribution.to_ascii_lowercase();
        match dist.as_str() {
            "modula" => Self::Modula(Modula::from(names.len())),
            "ketama" => Self::Consistent(Consistent::from(names)),
            "range" => Self::Range(Range::from(names.len())),
            _ => {
                log::warn!("'{}' is not valid , use modula instead", distribution);
                Self::Modula(Modula::from(names.len()))
            }
        }
    }
    #[inline(always)]
    pub fn index(&self, hash: u64) -> usize {
        match self {
            Self::Consistent(d) => d.index(hash),
            Self::Modula(d) => d.index(hash),
            Self::Range(r) => r.index(hash),
        }
    }
}
