mod consistent;
use consistent::Consistent;

mod modula;
use modula::Modula;

#[derive(Clone)]
pub enum Distribute {
    Consistent(Consistent),
    Modula(Modula),
}

impl Distribute {
    pub fn from(distribution: &str, names: &Vec<String>) -> Self {
        match distribution {
            "modula" | "Modula" | "MODULA" => Self::Modula(Modula::from(names.len())),
            "ketama" | "Ketama" | "KETAMA" => Self::Consistent(Consistent::from(&names)),
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
        }
    }
}
