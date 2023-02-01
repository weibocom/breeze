use ds::arena::Ephemera;
use std::sync::atomic::{AtomicUsize, Ordering::*};

struct V(usize);
static SEQ_CREATE: AtomicUsize = AtomicUsize::new(0);
static SEQ_DROP: AtomicUsize = AtomicUsize::new(0);
impl V {
    fn new(v: usize) -> Self {
        SEQ_CREATE.fetch_add(1, AcqRel);
        V(v)
    }
}
impl Drop for V {
    fn drop(&mut self) {
        SEQ_DROP.fetch_add(1, AcqRel);
    }
}
static ARENA: Ephemera<V, 4> = Ephemera::new();
#[test]
fn test_ephemera() {
    let v = ARENA.alloc(V::new(1));
    ARENA.dealloc(v);

    random_once();

    let threads = 4;
    let mut handlers = Vec::with_capacity(threads);
    for _ in 0..threads {
        handlers.push(std::thread::spawn(|| random_once()));
    }
    for h in handlers {
        h.join().expect("no error");
    }

    assert_eq!(SEQ_CREATE.load(Acquire), SEQ_DROP.load(Acquire));
}

// 随机创建n个对象，然后释放m(n<=m)个对象。
// 最后释放所有对象
fn random_once() {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut total = rng.gen::<u16>() as usize;
    let mut pending = std::collections::LinkedList::new();
    let mut seq = 0;

    while total > 0 {
        let create = rng.gen_range(1..=total);
        let mut free = rng.gen_range(0..=create);
        for _i in 0..=create {
            let v = seq;
            seq += 1;
            let ptr = ARENA.alloc(V::new(v));
            assert_eq!(unsafe { (&*ptr.as_ptr()).0 }, v);
            let drop_immediately: bool = rng.gen();
            if drop_immediately && free > 0 {
                ARENA.dealloc(ptr);
                free -= 1;
            } else {
                pending.push_back(ptr);
            }
        }
        for _i in 0..free {
            let ptr = pending.pop_back().expect("double free");
            ARENA.dealloc(ptr);
        }
        total -= create;
    }
    for ptr in pending {
        ARENA.dealloc(ptr);
    }
}
