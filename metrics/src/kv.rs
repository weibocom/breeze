// 所有的metric item都需要实现该接口
pub(crate) trait KvItem {
    // f: 第一个参数： sub_key; 第二个参数: 是提交到metric server的value。
    // secs: 是耗时。单位为秒
    fn with_item<F: Fn(&'static str, f64)>(&self, secs: f64, f: F);
}

pub(crate) trait KV {
    fn kv(&self, sid: usize, key: &str, sub_key: &str, v: f64);
}
