use std::sync::Arc;

use ds::chan::mpsc::{channel, Sender, TrySendError};

use ds::Switcher;

use crate::checker::BackendChecker;
use metrics::Path;
use protocol::{Endpoint, Error, Protocol, Request, Resource};

#[derive(Clone)]
pub struct BackendBuilder<P, R> {
    _marker: std::marker::PhantomData<(P, R)>,
}

use std::time::Duration;
impl<P: Protocol, R: Request> protocol::Builder<P, R, Arc<Backend<R>>> for BackendBuilder<P, R> {
    fn build(
        addr: &str,
        parser: P,
        rsrc: Resource,
        service: &str,
        timeout: Duration,
    ) -> Arc<Backend<R>> {
        let (tx, rx) = channel(256);
        let finish: Switcher = false.into();
        let init: Switcher = false.into();
        let f = finish.clone();
        let path_prefix = rsrc.name().to_string() + "_backend";
        let path = Path::new(vec![path_prefix.as_str(), service, addr]);
        let mut checker = BackendChecker::from(addr, rx, f, init.clone(), parser, path, timeout);
        rt::spawn(async move { checker.start_check().await });
        Backend { finish, init, tx }.into()
    }
}

pub struct Backend<R> {
    tx: Sender<R>,
    // 实例销毁时，设置该值，通知checker，会议上check.
    finish: Switcher,
    // 由checker设置，标识是否初始化完成。
    init: Switcher,
}

impl<R> discovery::Inited for Backend<R> {
    // 已经连接上或者至少连接了一次
    #[inline]
    fn inited(&self) -> bool {
        self.init.get()
    }
}

impl<R> Drop for Backend<R> {
    fn drop(&mut self) {
        self.finish.on();
    }
}

impl<R: Request> Endpoint for Backend<R> {
    type Item = R;
    #[inline]
    fn send(&self, req: R) {
        if let Err(e) = self.tx.try_send(req) {
            match e {
                TrySendError::Closed(r) => r.on_err(Error::ChanFull),
                TrySendError::Full(r) => r.on_err(Error::ChanFull),
                TrySendError::Disabled(r) => r.on_err(Error::ChanDisabled),
            }
        }
    }

    #[inline]
    fn shard_idx(&self, hash: i64) -> usize {
        log::warn!(
            "+++ should not come to Backend::shard_idx with hash: {}",
            hash
        );
        0
    }
}
