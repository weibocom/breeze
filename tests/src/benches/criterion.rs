use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn bench_get_checked(c: &mut Criterion) {
    let slice = [0u8; 1024];
    let mut group = c.benchmark_group("vec_get");
    group.bench_function("checked", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0;
                for i in 0..1024 {
                    t += *slice.get(i).unwrap_or(&0) as u64;
                }
                t
            });
        });
    });
    group.bench_function("unchecked", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0;
                for i in 0..1024 {
                    t += unsafe { slice.get_unchecked(i) };
                }
                t
            });
        });
    });
    group.finish();
}
fn bench_instant(c: &mut Criterion) {
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
    group.finish();
}
struct CustomDuration(u64);
impl CustomDuration {
    #[inline(always)]
    pub fn as_micros(&self) -> u64 {
        self.0 / 1_000
    }
}
fn bench_duration(c: &mut Criterion) {
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

//fn bench_get_uncheck(c: &mut Criterion) {
//    c.bench_function("minstant::Instant::as_unix_nanos()", |b| {
//        b.iter(|| {
//            black_box({});
//        });
//    });
//}

criterion_group!(benches, bench_get_checked, bench_instant, bench_duration);
criterion_main!(benches);
