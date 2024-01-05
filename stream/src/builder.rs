use std::sync::Arc;

use ds::chan::mpsc::{channel, Sender, TrySendError};

use ds::Switcher;

use crate::checker::BackendChecker;
use endpoint::{Endpoint, Timeout};
use metrics::Path;
use protocol::{Error, Protocol, Request, ResOption, Resource};

impl<R: Request, P: Protocol> From<(&str, P, Resource, &str, Timeout, ResOption)> for Backend<R> {
    fn from(
        (addr, parser, rsrc, service, timeout, option): (
            &str,
            P,
            Resource,
            &str,
            Timeout,
            ResOption,
        ),
    ) -> Self {
        let (tx, rx) = channel(256);
        let finish: Switcher = false.into();
        let init: Switcher = false.into();
        let f = finish.clone();
        let path = Path::new(vec![rsrc.name(), service]);
        let checker =
            BackendChecker::from(addr, rx, f, init.clone(), parser, path, timeout, option);
        rt::spawn(checker.start_check());

        let addr = addr.to_string();
        Backend {
            inner: BackendInner {
                addr,
                finish,
                init,
                tx,
            }
            .into(),
        }
    }
}

#[derive(Clone)]
pub struct Backend<R> {
    inner: Arc<BackendInner<R>>,
}

pub struct BackendInner<R> {
    addr: String,
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
        self.inner.init.get()
    }
}

impl<R> Drop for BackendInner<R> {
    fn drop(&mut self) {
        self.finish.on();
    }
}

impl<R: Request> Endpoint for Backend<R> {
    type Item = R;
    #[inline]
    fn send(&self, req: R) {
        if let Err(e) = self.inner.tx.try_send(req) {
            match e {
                TrySendError::Closed(r) => r.on_err(Error::ChanWriteClosed),
                TrySendError::Full(r) => r.on_err(Error::ChanFull),
                TrySendError::Disabled(r) => r.on_err(Error::ChanDisabled),
            }
        }
    }

    #[inline]
    fn available(&self) -> bool {
        self.inner.tx.get_enable()
    }
    #[inline]
    fn addr(&self) -> &str {
        &self.inner.addr
    }
    fn build_o<P: Protocol>(
        addr: &str,
        p: P,
        r: Resource,
        service: &str,
        to: Timeout,
        o: ResOption,
    ) -> Self {
        Self::from((addr, p, r, service, to, o))
    }
}
