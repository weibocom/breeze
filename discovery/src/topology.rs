use ds::{cow, CowReadHandle, CowWriteHandle};
use protocol::Resource;

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

pub trait TopologyRead<T> {
    fn resource(&self) -> Resource;
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O;
}

pub trait TopologyWrite {
    fn resource(&self) -> Resource;
    fn update(&mut self, name: &str, cfg: &str, hosts: &HashMap<String, Vec<String>>);
    fn gc(&mut self);
}

#[derive(Clone)]
pub struct CowWrapper<T> {
    inner: T,
}

pub fn topology<T>(t: T, service: &str) -> (TopologyWriteGuard<T>, TopologyReadGuard<T>)
where
    T: TopologyWrite + Clone,
{
    let resource = t.resource();
    let (tx, rx) = cow(t);
    let name = service.to_string();
    let idx = name.find(':').unwrap_or(name.len());
    let mut path = name.clone().replace('+', "/");
    path.truncate(idx);

    let updates = Arc::new(AtomicUsize::new(0));

    (
        TopologyWriteGuard {
            inner: tx,
            resource: resource,
            name: name,
            path: path,
            updates: updates.clone(),
        },
        TopologyReadGuard {
            resource: resource,
            inner: rx,
            updates: updates,
        },
    )
}

pub trait Inited {
    fn inited(&self) -> bool;
}

unsafe impl<T> Send for TopologyReadGuard<T> {}
unsafe impl<T> Sync for TopologyReadGuard<T> {}
#[derive(Clone)]
pub struct TopologyReadGuard<T> {
    resource: Resource,
    updates: Arc<AtomicUsize>,
    inner: CowReadHandle<T>,
}
pub struct TopologyWriteGuard<T>
where
    T: Clone,
{
    inner: CowWriteHandle<T>,
    resource: Resource,
    name: String,
    path: String,
    updates: Arc<AtomicUsize>,
}

impl<T> TopologyRead<T> for TopologyReadGuard<T> {
    fn resource(&self) -> Resource {
        self.resource
    }
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        self.inner.read(|t| f(t))
    }
}

impl<T> TopologyReadGuard<T>
where
    T: Clone + Inited,
{
    #[inline]
    pub fn inited(&self) -> bool {
        self.updates.load(Ordering::Relaxed) > 0 && self.do_with(|t| t.inited())
    }
}

impl<T> TopologyWrite for TopologyWriteGuard<T>
where
    T: TopologyWrite + Clone,
{
    fn resource(&self) -> Resource {
        self.resource.clone()
    }
    fn update(&mut self, name: &str, cfg: &str, hosts: &HashMap<String, Vec<String>>) {
        log::info!("topology updating. name:{}, cfg len:{}", name, cfg.len());
        //self.inner.write(&(name.to_string(), cfg.to_string()));
        self.inner.write(|t| t.update(name, cfg, hosts));
        self.updates.fetch_add(1, Ordering::Relaxed);
    }
    fn gc(&mut self) {
        self.inner.write(|t| t.gc());
    }
}

impl<T> crate::ServiceId for TopologyWriteGuard<T>
where
    T: Clone,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn path(&self) -> &str {
        &self.path
    }
}

impl<T> TopologyRead<T> for Arc<TopologyReadGuard<T>> {
    fn resource(&self) -> Resource {
        return self.resource;
    }
    #[inline]
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        (**self).do_with(f)
    }
}

impl<T> TopologyReadGuard<T>
where
    T: Clone,
{
    pub fn tick(&self) -> TopologyTicker {
        TopologyTicker(self.updates.clone())
    }
}

// topology更新了多少次. 可以通过这个进行订阅更新通知
#[derive(Clone)]
pub struct TopologyTicker(Arc<AtomicUsize>);
impl TopologyTicker {
    #[inline]
    pub fn cycle(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}
