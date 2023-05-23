use ds::{cow, CowReadHandle, CowWriteHandle};

use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

// pub trait TopologyGroup {}

// pub trait TopologyRead<T> {
//     fn do_with<F, O>(&self, f: F) -> O
//     where
//         F: Fn(&T) -> O;
// }

pub trait TopologyWrite {
    fn update(&mut self, name: &str, cfg: &str);
    #[inline]
    fn disgroup<'a>(&self, _path: &'a str, cfg: &'a str) -> Vec<(&'a str, &'a str)> {
        vec![("", cfg)]
    }
    // 部分场景下，配置更新后，还需要load 命名服务，比如dns。
    #[inline]
    fn need_load(&self) -> bool {
        false
    }
    #[inline]
    fn load(&mut self) {}
}

pub fn topology<T>(t: T, service: &str) -> (TopologyWriteGuard<T>, TopologyReadGuard<T>)
where
    T: TopologyWrite + Clone,
{
    let (tx, rx) = cow(t);

    let updates = Arc::new(AtomicUsize::new(0));

    (
        TopologyWriteGuard {
            inner: tx,
            service: service.to_string(),
            updates: updates.clone(),
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

unsafe impl<T> Send for TopologyReadGuard<T> {}
unsafe impl<T> Sync for TopologyReadGuard<T> {}
#[derive(Clone)]
pub struct TopologyReadGuard<T> {
    updates: Arc<AtomicUsize>,
    inner: CowReadHandle<T>,
}
pub struct TopologyWriteGuard<T>
where
    T: Clone,
{
    inner: CowWriteHandle<T>,
    service: String,
    updates: Arc<AtomicUsize>,
}

// impl<T: Clone> TopologyRead<T> for TopologyReadGuard<T> {
//     fn do_with<F, O>(&self, f: F) -> O
//     where
//         F: Fn(&T) -> O,
//     {
//         self.inner.do_with(|t| f(t))
//     }
// }

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

impl<T> TopologyWrite for TopologyWriteGuard<T>
where
    T: TopologyWrite + Clone,
{
    fn update(&mut self, name: &str, cfg: &str) {
        self.inner.write(|t| {
            t.update(name, cfg);
            if t.need_load() {
                t.load();
            }
        });
        self.updates.fetch_add(1, Ordering::AcqRel);
    }
    #[inline]
    fn disgroup<'a>(&self, path: &'a str, cfg: &'a str) -> Vec<(&'a str, &'a str)> {
        self.inner.get().disgroup(path, cfg)
    }
    #[inline]
    fn need_load(&self) -> bool {
        self.inner.get().need_load()
    }
    #[inline]
    fn load(&mut self) {
        self.inner.write(|t| t.load());
        // 说明load完成
        if !self.need_load() {
            self.updates.fetch_add(1, Ordering::AcqRel);
        }
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

// impl<T: Clone> TopologyRead<T> for Arc<TopologyReadGuard<T>> {
//     #[inline]
//     fn do_with<F, O>(&self, f: F) -> O
//     where
//         F: Fn(&T) -> O,
//     {
//         (**self).do_with(f)
//     }
// }

// impl<T> TopologyReadGuard<T> {
//     #[inline]
//     pub fn cycle(&self) -> usize {
//         self.updates.load(Ordering::Acquire)
//     }
// }

// impl<'a, T> Deref for RefreshTopology<'a, T> {
//     type Target = T;
//     fn deref(&self) -> &Self::Target {
//         self.top.as_ref()
//     }
// }

// pub trait RefreshTop<T> {
//     fn get(&self) -> Arc<T>;
//     fn get_inner(&self) -> &T;
//     fn refresh(&mut self);
// }

// impl<T: Clone  + Inited> RefreshTop<T> for RefreshTopology<'_, T> {
//     fn get(&self) -> Arc<T> {
//         self.top.clone()
//     }
//     fn refresh(&mut self) {
//         self.top = self.reader.get();
//     }
//     fn get_inner(&self) -> &T {
//         &self.top
//     }
// }
