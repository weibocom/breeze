use ds::{Cow, CowReadHandle, CowWriteHandle};

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

// 更新top时的参数。
type TO = (String, String);

pub trait TopologyRead<T> {
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O;
}

pub trait TopologyWrite {
    fn update(&mut self, name: &str, cfg: &str);
}

#[derive(Clone)]
pub struct CowWrapper<T> {
    inner: T,
}

pub fn topology<T>(t: T, service: &str) -> (TopologyWriteGuard<T>, TopologyReadGuard<T>)
where
    T: TopologyWrite + Clone + ds::Update<(String, String)>,
{
    let (tx, rx) = ds::cow(t);
    let name = service.to_string();
    let idx = name.find(':').unwrap_or(name.len());
    let mut path = name.clone().replace('+', "/");
    path.truncate(idx);

    let init = Arc::new(AtomicBool::new(false));

    (
        TopologyWriteGuard {
            inner: tx,
            name: name,
            path: path,
            init: init.clone(),
        },
        TopologyReadGuard {
            inner: rx,
            init: init,
        },
    )
}

#[derive(Clone)]
pub struct TopologyReadGuard<T> {
    init: Arc<AtomicBool>,
    inner: CowReadHandle<Cow<T, TO>>,
}
pub struct TopologyWriteGuard<T>
where
    T: Clone + ds::Update<TO>,
{
    inner: CowWriteHandle<Cow<T, TO>, TO>,
    name: String,
    path: String,
    init: Arc<AtomicBool>,
}

impl<T> TopologyRead<T> for TopologyReadGuard<T> {
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        self.inner.read(|t| f(t))
    }
}

impl<T> TopologyReadGuard<T>
where
    T: Clone,
{
    pub fn inited(&self) -> bool {
        self.init.load(Ordering::Acquire)
    }
}

impl<T> TopologyWrite for TopologyWriteGuard<T>
where
    T: TopologyWrite + Clone + ds::Update<TO>,
{
    fn update(&mut self, name: &str, cfg: &str) {
        log::info!("topology updating. name:{}, cfg len:{}", name, cfg.len());
        self.inner.write((name.to_string(), cfg.to_string()));
        if !self.init.load(Ordering::Relaxed) {
            self.init.store(true, Ordering::Release);
        }
    }
}

impl<T> crate::ServiceId for TopologyWriteGuard<T>
where
    T: Clone + ds::Update<TO>,
{
    fn name(&self) -> &str {
        &self.name
    }
    fn path(&self) -> &str {
        &self.path
    }
}

impl<T> TopologyRead<T> for Arc<TopologyReadGuard<T>> {
    #[inline]
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        (**self).do_with(f)
    }
}
