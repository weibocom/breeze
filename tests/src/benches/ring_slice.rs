use criterion::{black_box, Criterion};
use ds::RingSlice;
pub(super) fn bench_iter(c: &mut Criterion) {
    let cap = 64;
    let slice = (0..cap).into_iter().map(|x| x as u8).collect::<Vec<u8>>();
    let len = slice.len();
    let mut group = c.benchmark_group("ring_slice_iter");
    let rs = RingSlice::from(slice.as_ptr(), slice.len(), 0, len);
    group.bench_function("vec", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0;
                for i in 0..len {
                    t += unsafe { *slice.get_unchecked(i) as u64 };
                }
                t
            });
        });
    });
    group.bench_function("at", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..rs.len() {
                    t += rs.at(i) as u64
                }
                t
            });
        });
    });
    group.bench_function("iter", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                let (first, sec) = rs.data();
                for v in first {
                    t += *v as u64;
                }
                for v in sec {
                    t += *v as u64;
                }
                t
            });
        });
    });
    group.bench_function("visit", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                rs.visit(|v| {
                    t += v as u64;
                });
                t
            });
        });
    });
    group.finish();
}
