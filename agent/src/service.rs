use context::Quadruple;
use net::Listener;
use rt::spawn;
use std::sync::Arc;
use std::time::Duration;

use discovery::TopologyWriteGuard;
use ds::chan::Sender;
use metrics::Path;
use protocol::{Parser, Result};
use stream::pipeline::copy_bidirectional;
use stream::Builder;
use stream::StreamMetrics;

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

    let mut listen_failed = Path::new(vec![quard.protocol(),&quard.biz()]).status("listen_failed");

    // 等待初始化完成
    let mut tries = 0usize;
    while !rx.inited() {
        tries += 1;
        let sleep = if tries <= 10 {
            Duration::from_secs(1)
        } else {
            // 拉取配置失败，业务监听失败数+1
            listen_failed += 1;
            log::warn!("waiting inited. {} tries:{}", quard, tries);
            // Duration::from_secs(1 << (tries.min(10)))
            // 1 << 10 差不多20分钟，太久了，先改为递增间隔 fishermen
            let mut t = 2 * (tries - 10) as u64;
            if t > 1024 {
                t = 1024;
            }
            Duration::from_secs(t)
        };
        tokio::time::sleep(sleep).await;
    }

    log::info!("service inited. {} ", quard);
    let switcher = ds::Switcher::from(true);
    let top = Arc::new(RefreshTopology::from(rx));
    let path = Path::new(vec![quard.protocol(), &quard.biz()]);

    // 服务注册完成，侦听端口直到成功。
    while let Err(_e) = _process_one(quard, &p, &top, &path).await {
        // 监听失败或accept连接失败，对监听失败数+1
        listen_failed += 1;
        log::warn!("service process failed. {}, err:{:?}", quard, _e);
        tokio::time::sleep(Duration::from_secs(6)).await;
    }
    switcher.off();

    // 因为回调，有可能在连接释放的时候，还在引用top。
    tokio::time::sleep(Duration::from_secs(3)).await;
    Ok(())
}

use endpoint::RefreshTopology;
async fn _process_one(
    quard: &Quadruple,
    p: &Parser,
    top: &Arc<RefreshTopology<Topology>>,
    path: &Path,
) -> Result<()> {
    let l = Listener::bind(&quard.family(), &quard.address()).await?;
    log::info!("started. {}", quard);

    loop {
        // 等待初始化成功
        let (client, _addr) = l.accept().await?;
        let client = rt::Stream::from(client);
        let p = p.clone();
        let metrics = StreamMetrics::new(path);
        let _path = format!("{:?}", path);
        log::debug!("connection established:{:?}", _path);
        let ctop;
        loop {
            if let Some(t) = top.build() {
                ctop = Some(t);
                break;
            }
            log::info!("build top failed, try later:{}", quard.service());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let top = ctop.expect("build failed");
        spawn(async move {
            if let Err(e) = copy_bidirectional(top, metrics, client, p).await {
                match e {
                    //protocol::Error::Quit => {} // client发送quit协议退出
                    //protocol::Error::ReadEof => {}
                    _e => log::debug!("{:?} disconnected. {:?}", _path, _e),
                }
            }
        });
    }
}

use tokio::net::TcpListener;
// 监控一个端口，主要用于进程监控
pub(super) async fn listener_for_supervisor(port: u16) -> Result<TcpListener> {
    let addr = format!("127.0.0.1:{}", port);
    let l = TcpListener::bind(&addr).await?;
    Ok(l)
}
