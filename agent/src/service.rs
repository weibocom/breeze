use net::listener::Listener;
use std::sync::Arc;
use std::time::Duration;
use tokio::spawn;

use context::Quadruple;
use crossbeam_channel::Sender;
use discovery::TopologyWriteGuard;
use metrics::Path;
use protocol::callback::{Callback, CallbackPtr};
use protocol::{Parser, Result};
use stream::pipeline::copy_bidirectional;
use stream::Builder;

use stream::Request;
type Endpoint = Arc<stream::Backend<Request>>;
type Topology = endpoint::Topology<Builder<Parser, Request>, Endpoint, Request, Parser>;
// 一直侦听，直到成功侦听或者取消侦听（当前尚未支持取消侦听）
// 1. 尝试侦听之前，先确保服务配置信息已经更新完成
pub(super) async fn process_one(
    quard: &Quadruple,
    discovery: Sender<TopologyWriteGuard<Topology>>,
) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let p = Parser::try_from(&quard.protocol())?;
    let top = endpoint::Topology::try_from(p.clone(), quard.endpoint())?;
    let (tx, rx) = discovery::topology(top, &quard.service());
    // 注册，定期更新配置
    discovery.send(tx)?;

    // 等待初始化完成
    let mut tries = 0usize;
    while !rx.inited() {
        if tries >= 2 {
            log::warn!("waiting inited. {} ", quard);
        }
        tokio::time::sleep(Duration::from_secs(1 << (tries.min(10)))).await;
        tries += 1;
    }
    log::debug!("service inited. {} ", quard);
    let switcher = ds::Switcher::from(true);
    let top = Arc::new(RefreshTopology::new(rx, switcher.clone()));
    let receiver = top.as_ref() as *const RefreshTopology<Topology> as usize;
    let cb = RefreshTopology::<Topology>::static_send;
    let path = Path::new(vec![quard.protocol(), &quard.biz()]);
    let cb = Callback::new(receiver, cb);
    let cb_ptr: CallbackPtr = (&cb).into();

    // 服务注册完成，侦听端口直到成功。
    while let Err(e) = _process_one(quard, &p, &top, cb_ptr.clone(), &path).await {
        log::warn!("service process failed. {}, err:{:?}", quard, e);
        tokio::time::sleep(Duration::from_secs(6)).await;
    }
    switcher.off();

    // TODO 延迟一秒，释放top内存。
    // 因为回调，有可能在连接释放的时候，还在引用top。
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(())
}

use endpoint::RefreshTopology;
async fn _process_one(
    quard: &Quadruple,
    p: &Parser,
    top: &Arc<RefreshTopology<Topology>>,
    cb: CallbackPtr,
    path: &Path,
) -> Result<()> {
    let l = Listener::bind(&quard.family(), &quard.address()).await?;

    log::info!("service started. {}", quard);
    use stream::StreamMetrics;

    loop {
        let top = top.clone();
        // 等待初始化成功
        let (client, _addr) = l.accept().await?;
        let p = p.clone();
        let cb = cb.clone();
        let metrics = StreamMetrics::new(path);
        spawn(async move {
            use protocol::Topology;
            let hasher = top.hasher();
            use protocol::Error;
            if let Err(e) = copy_bidirectional(cb, metrics, hasher, client, p).await {
                match e {
                    Error::Quit => {} // client发送quit协议退出
                    Error::ReadEof => {}
                    e => log::debug!("disconnected. {:?} ", e),
                }
            }
        });
    }
}

use tokio::net::TcpListener;
// 监控一个端口，主要用于进程监控
pub(super) async fn listener_for_supervisor(port: u16) -> Result<TcpListener> {
    let addr = format!("10.222.76.140:{}", port);
    let l = TcpListener::bind(&addr).await?;
    Ok(l)
}
