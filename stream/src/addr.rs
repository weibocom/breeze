#[derive(Debug, Eq, Hash)]
pub struct Address {
    inner: String,
}
impl Address {
    pub fn append(&mut self, other: Address) {
        self.inner.push_str(" | ");
        self.inner.push_str(&other.inner);
    }

    pub fn to_string(&self) -> String {
        self.inner.clone()
    }
}
impl From<String> for Address {
    fn from(addr: String) -> Self {
        Address { inner: addr }
    }
}
impl Addressed for Address {
    fn addr(&self) -> Address {
        Self {
            inner: self.inner.to_owned(),
        }
    }
}
pub trait Addressed {
    fn addr(&self) -> Address;
}

impl<T> Addressed for Vec<T>
where
    T: Addressed,
{
    fn addr(&self) -> Address {
        if self.len() == 0 {
            Address {
                inner: String::new(),
            }
        } else {
            let mut s = String::with_capacity(16 * self.len());
            s.push('{');
            let mut s = self.iter().fold(s, |mut s, e| {
                s.push_str(e.addr().inner.as_str());
                s.push(',');
                s
            });
            // 把最后一个逗号替换成'}'
            s.pop();
            s.push('}');
            Address { inner: s }
        }
    }
}

pub trait Names {
    fn names(&self) -> Vec<String>;
}

impl<S> Names for Vec<S>
where
    S: Addressed,
{
    #[inline]
    fn names(&self) -> Vec<String> {
        self.iter().map(|s| s.addr().inner).collect()
    }
}

impl PartialEq for Address {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.inner == other.inner
    }
}

pub trait FakedClone {
    fn faked_clone(&self) -> Self;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum LayerRole {
    MasterL1 = 0,
    Master = 1,
    Slave = 2,
    SlaveL1 = 3,
    Noreply = 9,
    Unknow = 10,
}

impl From<usize> for LayerRole {
    fn from(layer_idx: usize) -> Self {
        match layer_idx {
            0 => LayerRole::MasterL1,
            1 => LayerRole::Master,
            2 => LayerRole::Slave,
            3 => LayerRole::SlaveL1,
            10 => LayerRole::Noreply,
            _ => {
                log::error!("Error: unknow layer_idx:{}", layer_idx);
                debug_assert!(false);
                LayerRole::Unknow
            }
        }
    }
}

pub trait LayerRoleAble {
    fn layer_role(&self) -> LayerRole;
    fn is_master(&self) -> bool;
}
