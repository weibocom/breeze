use criterion::{black_box, Criterion};
use ds::RingSlice;
pub(super) fn bench_iter(c: &mut Criterion) {
    let slice = (0..64).into_iter().map(|x| x as u8).collect::<Vec<u8>>();
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
    group.bench_function("iter", |b| {
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
    group.finish();
}
