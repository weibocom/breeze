use crate::Config;
// 提供一个除了topology之外的，定期使用discovery的策略
#[derive(Default)]
pub struct Fixed {
    // name, sig, callback
    cbs: Vec<(String, String, Box<dyn Fn(&str) + 'static + Send + Sync>)>,
}

impl Fixed {
    pub(crate) async fn with_discovery<D: crate::Discover>(&mut self, discovery: &D) {
        for (name, sig, cb) in self.cbs.iter_mut() {
            match discovery.get_service::<String>(name, sig).await {
                Ok(Config::Config(new_sig, cfg)) => {
                    *sig = new_sig;
                    cb(&cfg);
                }
                _ => {}
            }
        }
    }
    pub fn register(&mut self, path: String, cb: impl Fn(&str) + 'static + Send + Sync) {
        self.cbs.push((path, "".to_string(), Box::new(cb)));
    }
}

unsafe impl Send for Fixed {}
unsafe impl Sync for Fixed {}
