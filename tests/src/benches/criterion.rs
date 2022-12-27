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

//fn bench_get_uncheck(c: &mut Criterion) {
//    c.bench_function("minstant::Instant::as_unix_nanos()", |b| {
//        b.iter(|| {
//            black_box({});
//        });
//    });
//}

criterion_group!(benches, bench_get_checked);
criterion_main!(benches);
