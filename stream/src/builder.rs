use std::sync::Arc;

use tokio::sync::mpsc::channel;
use tokio::sync::mpsc::{error::TrySendError, Sender};

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
        let run: Switcher = false.into();
        let f = finish.clone();
        let path = Path::new(vec![rsrc.name(), service, addr]);
        let r = run.clone();
        let mut checker =
            BackendChecker::from(addr, rx, r, f, init.clone(), parser, &path, timeout);
        rt::spawn(async move { checker.start_check().await });
        Backend {
            finish,
            init,
            run,
            tx,
        }
        .into()
    }
}

pub struct Backend<R> {
    tx: Sender<R>,
    // 实例销毁时，设置该值，通知checker，会议上check.
    finish: Switcher,
    // 由checker设置，标识是否初始化完成。
    init: Switcher,
    // 由checker设置，标识当前资源是否在提供服务。
    run: Switcher,
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
    #[inline(always)]
    fn send(&self, req: R) {
        if self.run.get() {
            if let Err(e) = self.tx.try_send(req) {
                match e {
                    TrySendError::Closed(r) => r.on_err(Error::QueueFull),
                    TrySendError::Full(r) => r.on_err(Error::QueueFull),
                }
            }
        } else {
            req.on_err(Error::Closed);
        }
    }
}
