use ds::{Cow, CowReadHandle, CowWriteHandle};

// 更新top时的参数。
type TO = (String, String);

pub trait TopologyRead<T> {
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O;
}
//vhjcedjwpssbq
//wbhjc1d2i
///ewrenjd2`k1psq[l
/// //2ec1 boniepd2olmqaz]
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
    let mut path = name.clone().replace('+', "/");
    let idx = path.find(':').unwrap_or(path.len());
    path.truncate(idx);

    (
        TopologyWriteGuard {
            inner: tx,
            name: name,
            path: path,
        },
        TopologyReadGuard { inner: rx },
    )
}

#[derive(Clone)]
pub struct TopologyReadGuard<T> {
    inner: CowReadHandle<Cow<T, TO>>,
}
pub struct TopologyWriteGuard<T>
where
    T: Clone + ds::Update<TO>,
{
    inner: CowWriteHandle<Cow<T, TO>, TO>,
    name: String,
    path: String,
}

impl<T> TopologyRead<T> for TopologyReadGuard<T> {
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        self.inner.read(|t| f(t))
    }
}

impl<T> TopologyWrite for TopologyWriteGuard<T>
where
    T: TopologyWrite + Clone + ds::Update<TO>,
{
    fn update(&mut self, name: &str, cfg: &str) {
        log::info!("topology updateing. name:{}, cfg len:{}", name, cfg.len());
        self.inner.write((name.to_string(), cfg.to_string()));
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

use std::sync::Arc;
impl<T> TopologyRead<T> for Arc<TopologyReadGuard<T>> {
    #[inline]
    fn do_with<F, O>(&self, f: F) -> O
    where
        F: Fn(&T) -> O,
    {
        (**self).do_with(f)
    }
}
