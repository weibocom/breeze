use protocol::Resource;
use std::sync::atomic::Ordering;
use stream::LayerRole;

use crate::{cacheservice::MemcacheNamespace, redisservice::RedisNamespace, seq::Seq};

// struct Seq {
//     inner: AtomicUsize,
// }
// impl std::ops::Deref for Seq {
//     type Target = AtomicUsize;
//     #[inline]
//     fn deref(&self) -> &Self::Target {
//         &self.inner
//     }
// }
// impl Default for Seq {
//     fn default() -> Self {
//         Self::random()
//     }
// }
// impl Clone for Seq {
//     fn clone(&self) -> Self {
//         Self {
//             inner: AtomicUsize::new(self.load(Ordering::Acquire)),
//         }
//     }
// }
// impl Seq {
//     fn random() -> Self {
//         // 一般情况下, 一层的sharding数量不会超过64k。
//         let rd = rand::thread_rng().gen_range(0..65536);
//         Self {
//             inner: AtomicUsize::new(rd),
//         }
//     }
// }
#[derive(Clone)]
pub struct Layer {
    resource: Resource,
    seq: Seq,
    l0: Vec<Vec<String>>,              // 包含master, master-l1
    l1: Vec<String>,                   // master,
    l2: Vec<(LayerRole, Vec<String>)>, // slave，对于mc只有一组，对于redis有多组
}

impl Default for Layer {
    fn default() -> Self {
        Layer {
            resource: Resource::Memcache,
            seq: Default::default(),
            l0: Default::default(),
            l1: Default::default(),
            l2: Default::default(),
        }
    }
}

impl super::VisitAddress for Layer {
    fn visit<F: FnMut(&str)>(&self, mut f: F) {
        self.l0
            .iter()
            .for_each(|group| group.iter().for_each(|addr| f(addr)));
        // l1 已经包含在l0中。无须再进行遍历
        self.l2.visit(f);
    }
    // 先从l0中随机选择一组（第0组是master）；
    // 如果从l0选择的一组不是第0组，再选择l1
    // 选择l2
    fn select<F: FnMut(LayerRole, usize, &str)>(&self, mut f: F) {
        // TODO: 对于redis，这里需要按线上调整，迁移redisservice/topology中的对应逻辑过来 fishermen
        match self.resource {
            Resource::Redis => return,
            _ => {}
        }
        assert!(self.l0.len() > 0);
        let l0_idx = self.seq.fetch_add(1, Ordering::AcqRel) % self.l0.len();
        let mut pool_idx = 0;
        unsafe {
            self.l0.get_unchecked(l0_idx).iter().for_each(|addr| {
                // 第0组L1是master
                if l0_idx != 0 {
                    f(LayerRole::MasterL1, pool_idx, addr);
                } else {
                    f(LayerRole::Master, pool_idx, addr);
                }
            });
        }
        if l0_idx > 0 && self.l1.len() > 0 {
            pool_idx += 1;
            self.l1
                .iter()
                .for_each(|addr| f(LayerRole::Master, pool_idx, addr));
        }
        // 目前分块下，slave放到了l1下。因此可能重复
        if self.l2.len() > 0 && self.l0[l0_idx] != self.l2[0].1 {
            pool_idx += 1;
            self.l2[0]
                .1
                .iter()
                .for_each(|addr| f(LayerRole::SlaveL1, pool_idx, addr));
        }
    }
}

impl Layer {
    pub(crate) fn update_for_memcache(&mut self, ns: &MemcacheNamespace) {
        // l0: master, master-l1
        self.l0.clear();
        self.l0.push(ns.master.clone());
        self.l0.extend(ns.master_l1.clone());

        // master
        self.l1 = ns.master.clone();
        // slave
        self.l2.clear();
        self.l2.push((LayerRole::Slave, ns.slave.clone()));
    }

    pub(crate) fn update_for_redis(&mut self, ns: &RedisNamespace) {
        debug_assert!(self.l1.len() == 0);
        self.l1 = ns.master.clone();

        self.l2.clear();
        for p in ns.slaves.clone() {
            self.l2.push((LayerRole::Slave, p));
        }
    }
}
