//// 所有的metric item都需要实现该接口
//pub(crate) trait KvItem: Clone {
//    // f: 第一个参数： sub_key; 第二个参数: 是提交到metric server的value。
//    // secs: 是耗时。单位为秒
//    fn with_item<F: Fn(&'static str, f64)>(&self, secs: f64, f: F);
//    // 在SnapshotItem中，每个统计周期结束后，是否要将存量数据进行清理。
//    #[inline(always)]
//    fn clear() -> bool {
//        true
//    }
//}
//
//pub(crate) trait KV {
//    fn kv(&self, sid: usize, key: &str, sub_key: &str, v: f64);
//}
