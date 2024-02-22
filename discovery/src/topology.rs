use ds::{cow, CowReadHandle, CowWriteHandle};
use metrics::{Metric, Path};

use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::path::GetNamespace;

pub trait TopologyWrite {
    fn update(&mut self, name: &str, cfg: &str) -> bool;
    #[inline]
    fn disgroup<'a>(&self, _path: &'a str, cfg: &'a str) -> Vec<(&'a str, &'a str)> {
        vec![("", cfg)]
    }
    // 部分场景下，配置更新后，还需要load 命名服务，比如dns。
    #[inline]
    fn need_load(&self) -> bool {
        false
    }
    //返回load代表当前top可用，否则是不可用状态，可能需要继续load
    #[inline]
    fn load(&mut self) -> bool {
        true
    }
}

pub fn topology<T>(t: T, service: &str) -> (TopologyWriteGuard<T>, TopologyReadGuard<T>)
where
    T: TopologyWrite + Clone,
{
    let (tx, rx) = cow(t);

    let updates = Arc::new(AtomicUsize::new(0));

    let path = Path::new(vec!["any", service.namespace()]);
    (
        TopologyWriteGuard {
            updating: None,
            inner: tx,
            service: service.to_string(),
            updates: updates.clone(),
            update_num: path.num("top_updated"),
        },
        TopologyReadGuard { inner: rx, updates },
    )
}

pub trait Inited {
    fn inited(&self) -> bool;
}
impl<T: Inited> Inited for std::sync::Arc<T> {
    #[inline]
    fn inited(&self) -> bool {
        (&**self).inited()
    }
}
impl<T: Inited, O> Inited for (T, O) {
    #[inline]
    fn inited(&self) -> bool {
        self.0.inited()
    }
}

#[derive(Clone)]
pub struct TopologyReadGuard<T> {
    updates: Arc<AtomicUsize>,
    inner: CowReadHandle<T>,
}
pub struct TopologyWriteGuard<T>
where
    T: Clone,
{
    updating: Option<T>,
    inner: CowWriteHandle<T>,
    service: String,
    updates: Arc<AtomicUsize>,
    update_num: Metric,
}

impl<T> Deref for TopologyReadGuard<T> {
    type Target = CowReadHandle<T>;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> TopologyReadGuard<T> {
    #[inline]
    pub fn version(&self) -> usize {
        self.updates.load(Ordering::Acquire)
    }
}

impl<T> TopologyReadGuard<T>
where
    T: Clone + Inited,
{
    #[inline]
    pub fn inited(&self) -> bool {
        //这一层是否还有必要，只判断后面条件不行吗？
        self.updates.load(Ordering::Relaxed) > 0 && self.inner.get().inited()
    }
}

/// 记录Topology的更新状态，包括update是否成功，load是否成功
struct TopologyWriteStatus {
    updated: bool,
    loaded: bool,
}

impl TopologyWriteStatus {
    fn new(updated: bool, loaded: bool) -> Self {
        Self { updated, loaded }
    }
}

impl<T> TopologyWriteGuard<T>
where
    T: Clone,
{
    fn update_inner(&mut self, f: impl Fn(&mut T) -> TopologyWriteStatus) -> TopologyWriteStatus {
        self.update_num += 1;
        let mut t = self.updating.take().unwrap_or_else(|| self.inner.copy());
        let status = f(&mut t);

        // 如果updated为false，说明配置异常，且loaded肯定为false，忽略配置，直接返回
        if !status.updated {
            assert!(!status.loaded);
            return status;
        }

        // 如果updated为true，但loaed为false，保存t，稍后再试
        if !status.loaded {
            let _ = self.updating.insert(t);
            return status;
        }

        // updated、loaded都为true，则直接更新t
        self.inner.update(t);
        self.updates.fetch_add(1, Ordering::AcqRel);
        return status;
    }
}

impl<T> TopologyWrite for TopologyWriteGuard<T>
where
    T: TopologyWrite + Clone,
{
    fn update(&mut self, name: &str, cfg: &str) -> bool {
        let status = self.update_inner(|t| {
            let updated = t.update(name, cfg);
            if !updated {
                return TopologyWriteStatus::new(updated, false);
            }

            let loaded = !t.need_load() || t.load();
            TopologyWriteStatus::new(updated, loaded)
        });

        // 只要updated为true，则认为update成功，load可以稍后继续重试
        status.updated
    }
    #[inline]
    fn disgroup<'a>(&self, path: &'a str, cfg: &'a str) -> Vec<(&'a str, &'a str)> {
        self.inner.get().disgroup(path, cfg)
    }
    #[inline]
    fn need_load(&self) -> bool {
        if let Some(t) = &self.updating {
            t.need_load()
        } else {
            self.inner.get().need_load()
        }
    }
    #[inline]
    fn load(&mut self) -> bool {
        // load时，不需要update，所以updated直接设置为true即可
        let status = self.update_inner(|t| TopologyWriteStatus {
            updated: true,
            loaded: t.load(),
        });
        status.loaded
    }
}

impl<T> crate::ServiceId for TopologyWriteGuard<T>
where
    T: Clone,
{
    // 从name中获取不包含目录的base。
    fn service(&self) -> &str {
        &self.service
    }
}
