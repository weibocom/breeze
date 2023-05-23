use discovery::TopologyReadGuard;
use ds::ReadGuard;
use endpoint::{Endpoint, Topology};
use protocol::{
    callback::{Callback, CallbackPtr},
    request::Request,
};
use sharding::hash::{Hash, HashKey};

pub trait TopologyCheck: Sized {
    fn refresh(&mut self) -> bool;
    fn callback(&self) -> CallbackPtr;
}

pub struct CheckedTopology<T> {
    reader: TopologyReadGuard<T>,
    //快照版本号
    version: usize,
    //快照，定时刷新
    top: ReadGuard<T>,
    cb: CallbackPtr,
}

impl<T: Clone + Topology<Item = Request> + 'static> From<TopologyReadGuard<T>>
    for CheckedTopology<T>
{
    fn from(reader: TopologyReadGuard<T>) -> Self {
        //version 可以小于get出来的版本，所以要先取
        let version = reader.version();
        // reader一定是已经初始化过的，否则会UB
        assert!(version > 0);
        let top = reader.get();
        let cb_top = top.clone();
        let send = Box::new(move |req| cb_top.send(req));
        let cb = Callback::new(send).into();
        Self {
            reader,
            version,
            top,
            cb,
        }
    }
}

impl<T: Clone + Topology<Item = Request> + 'static> TopologyCheck for CheckedTopology<T> {
    fn refresh(&mut self) -> bool {
        let version = self.reader.version();
        //大部分情况下都不需要更新
        if version > self.version {
            *self = Self::from(self.reader.clone());
            true
        } else {
            false
        }
    }
    fn callback(&self) -> CallbackPtr {
        self.cb.clone()
    }
}

impl<T: Endpoint> Endpoint for CheckedTopology<T> {
    type Item = T::Item;
    #[inline(always)]
    fn send(&self, req: T::Item) {
        self.top.send(req);
    }

    #[inline(always)]
    fn shard_idx(&self, hash: i64) -> usize {
        self.top.shard_idx(hash)
    }
}

impl<T: Topology> Hash for CheckedTopology<T> {
    #[inline(always)]
    fn hash<K: HashKey>(&self, k: &K) -> i64 {
        self.top.hash(k)
    }
}

impl<T: Topology> Topology for CheckedTopology<T> {
    #[inline(always)]
    fn exp_sec(&self) -> u32 {
        self.top.exp_sec()
    }
}
