use discovery::distance::Addr;
use endpoint::{select::Distance, Backend, Endpoint};
struct TBackend {
    addr: String,
    available: bool,
}

impl Addr for TBackend {
    fn addr(&self) -> &str {
        &self.addr
    }
}

impl Backend for TBackend {
    fn available(&self) -> bool {
        self.available
    }
}

impl Endpoint for TBackend {
    type Item = usize;

    fn send(&self, _req: Self::Item) {
        todo!()
    }
}

impl TBackend {
    fn new(addr: String, available: bool) -> Self {
        Self { addr, available }
    }
}

//全部都是local，都可用，则轮询
#[test]
#[should_panic]
fn select_all_local() {
    let mut shards = Distance::new();
    shards.update(
        vec![
            TBackend::new("127.0.0.1".to_string(), true),
            TBackend::new("127.0.0.2".to_string(), true),
            TBackend::new("127.0.0.3".to_string(), true),
            TBackend::new("127.0.0.4".to_string(), true),
        ],
        4,
        true,
    );
    assert_eq!(shards.select_next_idx(2, 1), 3);
    assert_eq!(shards.select_next_idx(3, 2), 0);
    assert_eq!(shards.select_next_idx(0, 3), 1);
    shards.select_next_idx(1, 4);
}

//部分是local，都可用，则在各自区域轮询
#[test]
fn select_some_local() {
    let mut shards = Distance::new();
    shards.update(
        vec![
            TBackend::new("127.0.0.1".to_string(), true),
            TBackend::new("127.0.0.2".to_string(), true),
            TBackend::new("127.0.0.3".to_string(), true),
            TBackend::new("127.0.0.4".to_string(), true),
        ],
        2,
        true,
    );
    assert_eq!(shards.select_next_idx(3, 1), 2);
    assert_eq!(shards.select_next_idx(2, 2), 3);
    assert_eq!(shards.select_next_idx(3, 3), 2);

    assert_eq!(shards.select_next_idx(0, 1), 1);
    assert_eq!(shards.select_next_idx(1, 2), 0);
    assert_eq!(shards.select_next_idx(0, 3), 1);
}

//全部都是local，但部分不可用，则轮询可用
#[test]
fn select_all_local_some_noava() {
    let mut shards = Distance::new();
    shards.update(
        vec![
            TBackend::new("127.0.0.1".to_string(), false),
            TBackend::new("127.0.0.2".to_string(), true),
            TBackend::new("127.0.0.3".to_string(), true),
            TBackend::new("127.0.0.4".to_string(), true),
        ],
        4,
        true,
    );
    assert_eq!(shards.select_next_idx(2, 1), 3);
    assert_eq!(shards.select_next_idx(3, 2), 1);
    assert_eq!(shards.select_next_idx(1, 3), 2);
}

//部分是local，但local全部不可用，则随机选择一个非local作为起始
#[test]
fn select_some_local_alllocal_noava() {
    let mut shards = Distance::new();
    shards.update(
        vec![
            TBackend::new("127.0.0.1".to_string(), false),
            TBackend::new("127.0.0.2".to_string(), false),
            TBackend::new("127.0.0.3".to_string(), true),
            TBackend::new("127.0.0.4".to_string(), true),
        ],
        2,
        true,
    );
    assert_eq!(shards.select_next_idx(3, 1), 2);
    assert_eq!(shards.select_next_idx(2, 2), 3);
    assert_eq!(shards.select_next_idx(3, 3), 2);

    assert!(shards.select_next_idx(0, 1) > 1);
    assert!(shards.select_next_idx(1, 2) > 1);
    assert_eq!(shards.select_next_idx(2, 3), 3);
}

//部分是local，但全部不可用，则选择下一个
#[test]
fn select_some_local_all_noava() {
    let mut shards = Distance::new();
    shards.update(
        vec![
            TBackend::new("127.0.0.1".to_string(), false),
            TBackend::new("127.0.0.2".to_string(), false),
            TBackend::new("127.0.0.3".to_string(), false),
            TBackend::new("127.0.0.4".to_string(), false),
        ],
        2,
        true,
    );
    assert_eq!(shards.select_next_idx(3, 1), 0);
    assert_eq!(shards.select_next_idx(0, 2), 1);
    assert_eq!(shards.select_next_idx(1, 3), 2);
}

//全部是local，但全部不可用，则选择下一个
#[test]
fn select_all_local_all_noava() {
    let mut shards = Distance::new();
    shards.update(
        vec![
            TBackend::new("127.0.0.1".to_string(), false),
            TBackend::new("127.0.0.2".to_string(), false),
            TBackend::new("127.0.0.3".to_string(), false),
            TBackend::new("127.0.0.4".to_string(), false),
        ],
        4,
        true,
    );
    assert_eq!(shards.select_next_idx(3, 1), 0);
    assert_eq!(shards.select_next_idx(0, 2), 1);
    assert_eq!(shards.select_next_idx(1, 3), 2);
}
