pub trait ServiceId {
    fn path(&self) -> &str;
}

// 服务名称是一个路径。路径里面如果，路径分隔符可能是'+'，需要替换成'/'。
// 另外，如果名称里面存在'#'，则'#'后面的内容并不是path的一部分，而是分隔类似cacheservice的命名空间
pub struct ServiceName {
    name: String,
}
impl ServiceName {
    pub fn from(name: String) -> Self {
        Self {
            name: name.replace('+', "/"),
        }
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ServiceId for ServiceName {
    fn path(&self) -> &str {
        let idx = self.name.find(':').unwrap_or(self.name.len());
        &self.name[..idx]
    }
}

impl ServiceId for &ServiceName {
    fn path(&self) -> &str {
        ServiceName::path(self)
    }
}

use std::ops::Deref;
impl Deref for ServiceName {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.name
    }
}
