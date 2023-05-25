use std::sync::{
    atomic::AtomicBool,
    atomic::Ordering::{Acquire, Release},
    Arc,
};

use ds::chan::mpsc::{channel, Sender, TrySendError};

use ds::Switcher;

use crate::checker::BackendChecker;
use endpoint::{Builder, Endpoint, Single, Timeout};
use metrics::Path;
use protocol::{request::Request, Error, Protocol, ResOption, Resource};

#[derive(Clone)]
pub struct BackendBuilder<P> {
    _marker: std::marker::PhantomData<P>,
}

impl<P: Protocol> Builder<P, Arc<Backend>> for BackendBuilder<P> {
    fn auth_option_build(
        addr: &str,
        parser: P,
        rsrc: Resource,
        service: &str,
        timeout: Timeout,
        option: ResOption,
    ) -> Arc<Backend> {
        let (tx, rx) = channel(256);
        let finish: Switcher = false.into();
        let init: Switcher = false.into();
        let f = finish.clone();
        let path = Path::new(vec![rsrc.name(), service]);
        let single = Arc::new(AtomicBool::new(false));
        let mut checker =
            BackendChecker::from(addr, rx, f, init.clone(), parser, path, timeout, option);
        let s = single.clone();
        rt::spawn(async move { checker.start_check(s).await });

        Backend {
            finish,
            init,
            tx,
            single,
        }
        .into()
    }
}

pub struct Backend {
    single: Arc<AtomicBool>,
    tx: Sender<Request>,
    // 实例销毁时，设置该值，通知checker，会议上check.
    finish: Switcher,
    // 由checker设置，标识是否初始化完成。
    init: Switcher,
}

impl discovery::Inited for Backend {
    // 已经连接上或者至少连接了一次
    #[inline]
    fn inited(&self) -> bool {
        self.init.get()
    }
}

impl Drop for Backend {
    fn drop(&mut self) {
        self.finish.on();
    }
}

impl Endpoint for Backend {
    #[inline]
    fn send(&self, req: Request) {
        if let Err(e) = self.tx.try_send(req) {
            match e {
                TrySendError::Closed(r) => r.on_err(Error::ChanFull),
                TrySendError::Full(r) => r.on_err(Error::ChanFull),
                TrySendError::Disabled(r) => r.on_err(Error::ChanDisabled),
            }
        }
    }
}
impl Single for Backend {
    fn single(&self) -> bool {
        self.single.load(Acquire)
    }
    fn enable_single(&self) {
        self.single.store(true, Release);
    }
    fn disable_single(&self) {
        self.single.store(false, Release);
    }
}
