use criterion::{black_box, Criterion};
pub(super) fn bench_instant(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_instant");
    group.bench_function("std", |b| {
        b.iter(|| {
            black_box({
                let start = std::time::Instant::now();
                start.elapsed().as_micros()
            });
        });
    });
    group.bench_function("minstant", |b| {
        b.iter(|| {
            black_box({
                let start = minstant::Instant::now();
                start.elapsed().as_micros()
            });
        });
    });
    group.bench_function("minstant_customize", |b| {
        b.iter(|| {
            black_box({
                let start = minstant::Instant::now();
                start.elapsed().as_micros()
            });
        });
    });
    group.bench_function("coarse", |b| {
        b.iter(|| {
            black_box({
                let start = ds::time::coarse::Instant::now();
                start.elapsed().as_micros()
            });
        });
    });
    group.bench_function("time", |b| {
        b.iter(|| {
            black_box({
                let start = time::Instant::now();
                start.elapsed().whole_microseconds()
            });
        });
    });
    group.finish();
}
struct CustomDuration(u64);
impl CustomDuration {
    #[inline(always)]
    pub fn as_micros(&self) -> u64 {
        self.0 / 1_000
    }
}
pub(super) fn bench_duration(c: &mut Criterion) {
    let mut group = c.benchmark_group("time_duration");
    group.bench_function("std", |b| {
        b.iter(|| black_box(std::time::Duration::from_nanos(1_000_000u64).as_micros()));
    });
    group.bench_function("u64", |b| {
        b.iter(|| {
            black_box(CustomDuration(1_000_000u64).as_micros());
        });
    });
    group.finish();
}
