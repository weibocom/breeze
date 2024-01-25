use discovery::distance::Addr;
use endpoint::{select::Distance, Endpoint};
struct TBackend {
    addr: String,
    available: bool,
}

impl Addr for TBackend {
    fn addr(&self) -> &str {
        &self.addr
    }
}

impl Endpoint for TBackend {
    type Item = usize;

    fn available(&self) -> bool {
        self.available
    }
    fn send(&self, _req: Self::Item) {
        todo!()
    }
}

impl TBackend {
    fn new(addr: String, available: bool) -> Self {
        Self { addr, available }
    }
}

//以下所有测试用例轮询顺序都是先local后非local，直到选到可用的
//全部都是local，都可用
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
    assert_eq!(shards.select_next_idx(1, 4), 2);
}

//部分是local，都可用
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
    assert_eq!(shards.select_next_idx(0, 1), 1);
    let non_local = shards.select_next_idx(2, 2);
    assert!(non_local > 1);
    assert!(shards.select_next_idx(non_local, 3) > 1);
}

//全部都是local，但部分不可用
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

//部分是local，但local全部不可用
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
    assert!(shards.select_next_idx(1, 1) > 1);
    assert_eq!(shards.select_next_idx(2, 2), 3);
    assert_eq!(shards.select_next_idx(3, 3), 2);
}

//部分是local，但全部不可用
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
    assert_eq!(shards.select_next_idx(1, 1), 2);
    assert_eq!(shards.select_next_idx(2, 2), 3);
    assert_eq!(shards.select_next_idx(3, 3), 0);
}

//全部是local，但全部不可用
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
