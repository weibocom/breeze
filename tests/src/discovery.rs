use std::{sync::Arc, thread};

use discovery::TopologyWrite;
use protocol::Parser;

#[test]
fn refresh() {
    use stream::{Backend, Builder, Request};
    type Endpoint = Arc<Backend<Request>>;
    type Topology = endpoint::TopologyProtocol<Builder<Parser, Request>, Endpoint, Request, Parser>;
    let service = "redisservice";
    let p = Parser::try_from("redis").unwrap();
    let top: Topology = endpoint::TopologyProtocol::try_from(p.clone(), "rs").unwrap();

    let cfg = r#"
backends:
- 1.1.1.1:1111,1.1.1.1:1111
basic:
    access_mod: rw
    distribution: modula
    hash: crc32local
    listen: 1111
    resource_type: eredis
    timeout_ms_master: 0
    timeout_ms_slave: 0"#;
    let cfgnew = r#"
backends:
- 1.1.1.1:2222,1.1.1.1:2222
basic:
    access_mod: rw
    distribution: modula
    hash: crc32local
    listen: 2222
    resource_type: eredis
    timeout_ms_master: 0
    timeout_ms_slave: 0"#;
    let (mut tx, rx) = discovery::topology(top, service);
    tx.update(service, cfg);
    let t_rx = rx.clone();
    thread::spawn(move || {
        assert_eq!(t_rx.get().get_backends(), vec!["1.1.1.1:1111,1.1.1.1:1111"]);
    });
    tx.update(service, cfgnew);
    let t_rx = rx.clone();
    thread::spawn(move || {
        assert_eq!(t_rx.get().get_backends(), vec!["1.1.1.1:2222,1.1.1.1:2222"]);
    });
}
