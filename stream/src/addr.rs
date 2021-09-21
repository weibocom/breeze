#[derive(Debug, Eq, Hash)]
pub struct Address {
    inner: String,
}
impl Address {
    pub fn append(&mut self, other: Address) {
        self.inner.push_str(" | ");
        self.inner.push_str(&other.inner);
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
