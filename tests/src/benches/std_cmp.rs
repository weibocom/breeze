use criterion::{black_box, Criterion};
pub(super) fn bench_num_to_str(c: &mut Criterion) {
    let mut group = c.benchmark_group("num_to_str");
    let end = 999;
    group.bench_function("to_string", |b| {
        b.iter(|| {
            black_box({
                let mut len = 0;
                for i in 0..=end {
                    len += i.to_string().len();
                }
                len
            });
        });
    });
    group.bench_function("tunning", |b| {
        b.iter(|| {
            black_box({
                use ds::NumStr;
                let mut len = 0;
                for i in 0..=end {
                    i.with_str(|s| len += s.len());
                }
                len
            });
        });
    });
    group.bench_function("loop_buf", |b| {
        b.iter(|| {
            black_box({
                let mut len = 0;
                for i in 0..=end {
                    ds::with_str(i, |s| len += s.len());
                }
                len
            });
        });
    });
    group.finish();
}
pub(super) fn bench_mod(c: &mut Criterion) {
    use rand::Rng;
    let mut r = rand::thread_rng();
    let (range, interval) = if r.gen::<bool>() {
        (1024usize, 32usize)
    } else {
        (512, 32)
    };
    let r_shift = range.trailing_zeros();
    let r_mask = range - 1;
    let s_shift = interval.trailing_zeros();
    let runs = 16usize;
    let shift = interval & (interval - 1) == 0;

    let mut group = c.benchmark_group("range_mod");
    group.bench_function("div_mod", |b| {
        b.iter(|| {
            black_box({
                let mut s = 0usize;
                for i in 0..runs {
                    s += ((i / range) % range) / interval;
                }
                s
            });
        });
    });
    group.bench_function("auto", |b| {
        b.iter(|| {
            black_box({
                let mut s = 0usize;
                if shift {
                    for i in 0..runs {
                        s += ((i >> r_shift) & r_mask) >> s_shift;
                    }
                } else {
                    for i in 0..runs {
                        s += ((i / range) % range) / interval;
                    }
                }
                s
            });
        });
    });
    group.bench_function("bit_ops", |b| {
        b.iter(|| {
            black_box({
                let mut s = 0usize;
                for i in 0..runs {
                    s += ((i >> r_shift) & r_mask) >> s_shift;
                }
                s
            });
        });
    });
    group.finish();
}
