use chrono::Local;
use ds::time::{Duration, Instant};
use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;

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
                let updated = self.update(&cfg, sig);
                if !updated {
                    log::warn!("+++ malformed snapshot:{} => {}", self.service(), cfg);
                }
            }
            Err(_e) => self.check_update(snapshot, discovery).await,
        }
    }

    /// 1 配置内容变化时，只有更新成功，才能更新snapshot；
    /// 2 把之前的配置临时输出到console，方便配置回滚
    pub(crate) async fn check_update<D: Cache>(&mut self, snapshot: &str, discovery: &mut D) {
        let service_name = self.service().name();
        if let Some((cfg, sig)) = discovery.get(&service_name, &self.sig, &self.inner).await {
            let mut dump = self.sig != sig;
            let update = self.sig.digest != sig.digest;
            if !dump && !update {
                return;
            }

            if update {
                log::info!("updating {:?} => {:?} cfg: {:?}", self, sig, cfg);
                dump = self.update(&cfg, sig);
            } else {
                self.sig = sig;
            }

            if dump {
                if let Ok((old_cfg, _sig)) = self.load_from_snapshot(&snapshot).await {
                    const TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S.%3f";
                    let time = Local::now().format(TIME_FORMAT).to_string();
                    // dump新配置前，先load出原配置，并写入console（临时方案，待配置回滚方案完善后，考虑去掉 fishermen #815）
                    println!("+++ {} {:?} will update cfg, old:{}", time, self, old_cfg);
                } else {
                    log::warn!("+++ load snapshot failed: {:?}", self);
                }

                self.dump(snapshot, &cfg).await;
            }
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
    /// 不管update是否成功，sig、last_update需要更新，避免异常配置之下，持续重复check
    /// 假设前提：一个配置，如果第一次update失败，不管重试几次，都会失败
    fn update(&mut self, cfg: &str, sig: Sig) -> bool {
        self.sig = sig;
        let name = self.service().namespace().to_string();
        self.last_update = Instant::now();
        self.inner.update(&name, cfg)
    }
    fn encoded_path(&self, snapshot: &str) -> PathBuf {
        let base = self.service().name();
        let mut pb = PathBuf::new();
        pb.push(snapshot);
        pb.push(base);
        pb
    }
    pub async fn load_from_snapshot(&self, name: &str) -> Result<(String, Sig)> {
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
        if let Err(_e) = self.try_dump(snapshot, cfg).await {
            log::warn!("failed to dump {:?} len:{} err:{:?}", self, cfg.len(), _e)
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
