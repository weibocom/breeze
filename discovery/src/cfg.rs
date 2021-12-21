use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{ServiceId, TopologyWrite};
use crate::cache::Cache;
use crate::path::{GetNamespace, ToName};
use crate::sig::Sig;
pub(crate) struct Config<T> {
    sig: Sig,
    inner: T,
    last_update: Instant, // 上一次更新的时间。
    last_load: Instant,
}

impl<T> From<T> for Config<T> {
    #[inline]
    fn from(inner: T) -> Self {
        Self {
            sig: Default::default(),
            inner,
            last_update: Instant::now(),
            last_load: Instant::now(),
        }
    }
}

impl<T> Config<T>
where
    T: Send + TopologyWrite + ServiceId + 'static + Sync,
{
    // 初始化。
    pub(crate) async fn init<C: Cache>(&mut self, snapshot: &str, discovery: &mut C) {
        match self.load_from_snapshot(snapshot).await {
            Ok((cfg, sig)) => {
                self.sig = sig;
                self.update(&cfg)
            }
            Err(_e) => self.check_update(snapshot, discovery).await,
        }
    }
    pub(crate) async fn check_update<D: Cache>(&mut self, snapshot: &str, discovery: &mut D) {
        let service = self.service();
        if let Some((cfg, sig)) = discovery.get(&service.name(), &self.sig, &self.inner).await {
            let dump = self.sig != sig;
            let update = self.sig.digest != sig.digest;
            if !dump && !update {
                return;
            }
            self.sig = sig;
            if update {
                self.update(&cfg);
            }
            self.dump(snapshot, &cfg).await;
        }
    }
    pub(crate) fn try_load(&mut self) {
        const CYCLE: Duration = Duration::from_secs(15);
        // 刚刚更新完成，或者长时间未load。则进行一次load
        if self.last_update.elapsed() <= CYCLE || self.last_load.elapsed() >= CYCLE {
            if self.need_load() {
                self.load();
                self.last_load = Instant::now();
            }
        }
    }
    fn update(&mut self, cfg: &str) {
        let name = self.service().namespace().to_string();
        log::info!("updating {:?}  namespace:{}", self, name);
        self.inner.update(&name, cfg);
        self.last_update = Instant::now();
    }
    fn encoded_path(&self, snapshot: &str) -> PathBuf {
        let base = self.service().name();
        let mut pb = PathBuf::new();
        pb.push(snapshot);
        pb.push(base);
        pb
    }
    async fn load_from_snapshot(&self, name: &str) -> Result<(String, Sig)> {
        let mut contents = Vec::with_capacity(8 * 1024);
        File::open(self.encoded_path(name))
            .await?
            .read_to_end(&mut contents)
            .await?;
        let mut contents = String::from_utf8(contents)
            .map_err(|_e| Error::new(ErrorKind::Other, "not a valid utf8 file"))?;
        // 内容的第一行是签名，第二行是往后是配置
        let idx = contents.find('\n').unwrap_or(0);
        let cfg = contents.split_off(idx);
        let sig: String = contents;
        let sig: Sig = sig.try_into()?;
        log::debug!("{} snapshot loaded:sig:{:?} cfg:{}", name, sig, cfg.len());
        Ok((cfg, sig))
    }
    async fn dump(&self, snapshot: &str, cfg: &str) {
        if let Err(e) = self.try_dump(snapshot, cfg).await {
            log::warn!("failed to dump {:?} len:{} err:{:?}", self, cfg.len(), e)
        }
    }
    async fn try_dump(&self, snapshot: &str, cfg: &str) -> Result<()> {
        let sig_str = self.sig.serialize();
        log::debug!("dump {:?} cfg:{} sig_str:{}", self, cfg.len(), sig_str);
        let path = self.encoded_path(snapshot);
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        let mut file = File::create(path).await?;
        file.write_all(sig_str.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.write_all(cfg.as_bytes()).await?;
        Ok(())
    }
}

use std::fmt::{self, Debug, Formatter};
impl<T: ServiceId> Debug for Config<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "sig:{:?} name:{:?}", self.sig, self.service())
    }
}

use std::ops::{Deref, DerefMut};
impl<T: ServiceId> Deref for Config<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl<T: ServiceId> DerefMut for Config<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
