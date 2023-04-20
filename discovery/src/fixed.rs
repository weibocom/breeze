use crate::Config;
// 提供一个除了topology之外的，定期使用discovery的策略
#[derive(Default)]
pub struct Fixed {
    // name, idc, sig, callback
    cbs: Vec<(
        String,
        &'static str,
        String,
        Box<dyn Fn(&str) + 'static + Send + Sync>,
    )>,
}

impl Fixed {
    pub(crate) async fn with_discovery<D: crate::Discover>(&mut self, discovery: &D) {
        for (name, name_ext, sig, cb) in self.cbs.iter_mut() {
            let mut use_name = false;
            //优先使用name_ext配置
            if !name_ext.is_empty() {
                let name = format!("{}/{}", name, name_ext);
                match discovery.get_service::<String>(&name, sig).await {
                    Ok(Config::Config(new_sig, cfg)) => {
                        *sig = new_sig;
                        cb(&cfg);
                    }
                    Ok(Config::NotFound) => {
                        use_name = true;
                        log::warn!("service not found {} {}", name, name_ext);
                    }
                    Ok(Config::NotChanged) => {}
                    //其余情况不能判定应使用name配置
                    _e => log::warn!("get service {} err {:?}", name, _e),
                }
            }
            if name_ext.is_empty() || use_name {
                match discovery.get_service::<String>(name, sig).await {
                    Ok(Config::Config(new_sig, cfg)) => {
                        *sig = new_sig;
                        cb(&cfg);
                    }
                    Ok(Config::NotChanged) => {}
                    _e => log::warn!("get service {} err {:?}", name, _e),
                }
            }
        }
    }
    pub fn register(
        &mut self,
        path: String,
        name_ext: &'static str,
        cb: impl Fn(&str) + 'static + Send + Sync,
    ) {
        self.cbs
            .push((path, name_ext, "".to_string(), Box::new(cb)));
    }
}

unsafe impl Send for Fixed {}
unsafe impl Sync for Fixed {}
