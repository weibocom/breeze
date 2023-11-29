use crate::Config;
// 提供一个除了topology之外的，定期使用discovery的策略
#[derive(Default)]
pub struct Fixed {
    inited: bool,
    // name, idc, sig, callback
    cbs: Vec<(
        Vec<String>, // 支持从多个path获取数据
        String,
        Box<dyn Fn(&str) + 'static + Send + Sync>,
    )>,
}

impl Fixed {
    pub(crate) async fn with_discovery<D: crate::Discover>(&mut self, discovery: &D) {
        let mut success = 0;
        for (paths, sig, cb) in self.cbs.iter_mut() {
            for path in paths {
                match discovery.get_service::<String>(&path, sig).await {
                    Ok(Config::Config(new_sig, cfg)) => {
                        *sig = new_sig;
                        cb(&cfg);
                        success += 1;
                        break;
                    }
                    Ok(Config::NotChanged) => {
                        success += 1;
                        break;
                    }
                    Ok(Config::NotFound) => {
                        log::warn!("service not found: {}", path);
                    }
                    //其余情况不能判定应使用name配置
                    _e => {
                        log::warn!("get service {} err {:?}", path, _e);
                        break;
                    }
                }
            }
        }
        if !self.inited {
            self.inited = success == self.cbs.len();
        }
    }
    pub fn register(
        &mut self,
        path: String,
        name_ext: &'static str,
        cb: impl Fn(&str) + 'static + Send + Sync,
    ) {
        let mut available_paths = Vec::new();
        if !name_ext.is_empty() {
            available_paths.push(format!("{}/{}", path, name_ext));
        }
        available_paths.push(path);
        self.cbs
            .push((available_paths, "".to_string(), Box::new(cb)));
    }
    pub(crate) fn inited(&self) -> bool {
        self.inited
    }
}
