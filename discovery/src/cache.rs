//use super::ServiceDiscover;
//
//pub struct CachedServiceDiscovery<I, D> {
//    cache: I,
//    _discovery: D,
//}
//
///// 提供一个I的缓存，定期进行更新
//impl<I, D> CachedServiceDiscovery<I, D> {
//    pub fn from_discovery(discovery: D) -> Self
//    where
//        I: From<String>,
//        D: ServiceDiscover,
//    {
//        println!("cache service from discovery.");
//        let cache: I = discovery.get();
//        Self {
//            cache: cache,
//            _discovery: discovery,
//        }
//    }
//
//    #[inline]
//    pub fn as_ref(&self) -> &I {
//        &self.cache
//    }
//    #[inline]
//    pub fn as_mut_ref(&mut self) -> &mut I {
//        &mut self.cache
//    }
//}
