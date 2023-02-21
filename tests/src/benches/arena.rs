use std::sync::atomic::{AtomicU64, Ordering::*};

use criterion::{black_box, Criterion};

use ds::arena::{Allocator, Heap};
pub(super) fn bench_alloc(c: &mut Criterion) {
    let mut group = c.benchmark_group("arena_alloc");
    let mut cache = ds::arena::Arena::with_cache(16, Heap);
    const RUNS: u64 = 4;
    group.bench_function("cache", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0;
                for i in 0..RUNS {
                    let data = cache.alloc(AtomicU64::new(i));
                    t += unsafe { data.as_ref().load(Relaxed) };
                    cache.dealloc(data);
                }
                t
            });
        });
    });
    let heap = Heap;
    group.bench_function("heap", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0;
                for i in 0..RUNS {
                    let data = heap.alloc(AtomicU64::new(i));
                    t += unsafe { data.as_ref().load(Relaxed) };
                    heap.dealloc(data);
                }
                t
            });
        });
    });
    group.finish();
}
