use discovery::TopologyWrite;
use metrics::tests::init_metrics_onlyfor_test;

#[derive(Clone)]
struct RandomLoad {
    need_load: usize, //需要load的次数
    count: usize,     //已经load的次数
}

impl RandomLoad {
    fn new(need_load: usize) -> Self {
        Self {
            need_load,
            count: 0,
        }
    }
}

impl TopologyWrite for RandomLoad {
    fn update(&mut self, _name: &str, cfg: &str) {
        self.need_load = cfg.parse().unwrap();
        self.count = 0;
    }

    fn need_load(&self) -> bool {
        self.count < self.need_load
    }

    fn load(&mut self) -> bool {
        self.count += 1;
        !self.need_load()
    }
}

#[test]
fn refresh() {
    let top = RandomLoad::new(0);
    let service = "service";
    let _w = init_metrics_onlyfor_test();
    let (mut tx, rx) = discovery::topology(top, service);

    assert_eq!(rx.get().need_load, 0);
    assert!(!tx.need_load());

    //第一次没load成功，所以还是还是上一版top.need_load=0
    tx.update(service, "2");
    assert_eq!(rx.get().need_load, 0);
    //第二次load成功
    assert!(tx.need_load());
    assert!(tx.load());
    assert!(!tx.need_load());
    assert_eq!(rx.get().need_load, 2);

    //不需要load的场景
    tx.update(service, "1");
    assert!(!tx.need_load());
    assert_eq!(rx.get().need_load, 1);
    //已经成功load后，load也会返回true
    assert!(tx.load());
    assert_eq!(rx.get().need_load, 1);

    //并发更新，只有最后一个生效
    tx.update(service, "2");
    tx.update(service, "3");
    assert!(tx.need_load());
    assert_eq!(rx.get().need_load, 1);
    assert!(!tx.load());
    assert!(tx.need_load());
    assert_eq!(rx.get().need_load, 1);
    assert!(tx.load());
    assert!(!tx.need_load());
    assert_eq!(rx.get().need_load, 3);
}
