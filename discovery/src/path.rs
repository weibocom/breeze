pub trait ToPath {
    fn path(&self) -> String;
}
pub trait ToName {
    fn name(&self) -> String;
}
pub trait GetNamespace {
    fn namespace(&self) -> &str;
    fn cat(&self, ns: &str) -> String;
}

impl<T: ToString> ToPath for T {
    #[inline]
    fn path(&self) -> String {
        let mut path = self.to_string();
        if path.find('+').is_some() {
            path = path.replace('+', "/");
        }
        // 去掉namespace。
        let idx = path.rfind(':').unwrap_or(path.len());
        let last_path_idx = path.rfind('/').unwrap_or(path.len());
        //配置可能是127.0.0.1:8080/config/to/config，这种情况不截断
        if last_path_idx < idx {
            path.truncate(idx);
        }
        path
    }
}
impl<T: ToString> ToName for T {
    #[inline]
    fn name(&self) -> String {
        let mut name = self.to_string();
        if name.find('/').is_some() {
            name = name.replace('/', "+");
        }
        name
    }
}
impl<T: AsRef<str>> GetNamespace for T {
    // 取base name。如果base name中有冒号区分，则取冒号后面的内容。
    #[inline]
    fn namespace(&self) -> &str {
        let name = self.as_ref();
        let base = name
            .rfind(|c| c == '+' || c == '/')
            .map(|idx| &name[idx + 1..])
            .unwrap_or_else(|| name);
        // 再看是否有namespace
        base.rfind(':')
            .map(|idx| &base[idx + 1..])
            .unwrap_or_else(|| base)
    }
    #[inline]
    fn cat(&self, ns: &str) -> String {
        if ns.len() > 0 {
            let name = self.as_ref().to_string();
            name + ":" + ns
        } else {
            self.as_ref().to_string()
        }
    }
}
