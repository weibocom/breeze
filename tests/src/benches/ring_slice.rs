use criterion::{black_box, Criterion};
use ds::{ByteOrder, RingSlice};
pub(super) fn bench_iter(c: &mut Criterion) {
    let cap = 128;
    // 前cap-4个字节是数字，后4个字节是非数字
    let slice = (0..cap)
        .into_iter()
        .enumerate()
        .map(|(idx, x)| if idx <= cap - 4 { x as u8 } else { 'a' as u8 })
        .collect::<Vec<u8>>();
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
    group.bench_function("index", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..rs.len() {
                    t += rs[i] as u64
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
    group.bench_function("fold", |b| {
        b.iter(|| {
            black_box({
                rs.fold(0u64, |t, v| {
                    *t += v as u64;
                })
            });
        });
    });
    group.bench_function("fold_oft", |b| {
        b.iter(|| {
            black_box({
                rs.fold_until(
                    0,
                    0u64,
                    |t, v| {
                        *t += v as u64;
                    },
                    |c| c > b'9' || c < b'0',
                )
            });
        });
    });
    group.bench_function("fold_oft-true", |b| {
        b.iter(|| {
            black_box({
                rs.fold_until(
                    0,
                    0u64,
                    |t, v| {
                        *t += v as u64;
                    },
                    |_| true,
                )
            });
        });
    });
    group.finish();
}
pub(super) fn bench_read_num(c: &mut Criterion) {
    let cap = 32;
    // 前cap-4个字节是数字，后4个字节是非数字
    let slice = (0..cap)
        .into_iter()
        .enumerate()
        .map(|(idx, x)| if idx <= cap - 4 { x as u8 } else { 'a' as u8 })
        .collect::<Vec<u8>>();
    let len = slice.len();
    let runs = 24;
    let mut group = c.benchmark_group("ring_slice_read_num");
    let rs = RingSlice::from(slice.as_ptr(), slice.len(), 0, len);
    group.bench_function("u64_le", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..runs {
                    t = t.wrapping_add(rs.read_u64_le(i) as u64);
                }
                t
            });
        });
    });
    group.bench_function("u64_le_procs", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..runs {
                    t = t.wrapping_add(rs.u64_le(i) as u64);
                }
                t
            });
        });
    });
    group.bench_function("u64_le_copy", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..runs {
                    let mut buf = [0u8; 8];
                    rs.copy_to_r(&mut buf[..], i..i + 8);
                    t = t.wrapping_add(u64::from_le_bytes(buf));
                }
                t
            });
        });
    });
    group.bench_function("u64_be", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..runs {
                    t = t.wrapping_add(rs.read_u64_be(i) as u64);
                }
                t
            });
        });
    });
    group.bench_function("u64_be_copy", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..runs {
                    let mut buf = [0u8; 8];
                    rs.copy_to_r(&mut buf[..], i..i + 8);
                    t = t.wrapping_add(u64::from_be_bytes(buf));
                }
                t
            });
        });
    });
    group.bench_function("u64_be_cmp", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..runs {
                    t = t.wrapping_add(rs.read_u64_be_cmp(i) as u64);
                }
                t
            });
        });
    });
    group.bench_function("u56_le", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..runs {
                    t = t.wrapping_add(rs.read_u56_le(i) as u64);
                }
                t
            });
        });
    });
    group.bench_function("u56_le_cmp", |b| {
        b.iter(|| {
            black_box({
                let mut t = 0u64;
                for i in 0..runs {
                    t = t.wrapping_add(rs.read_u56_le_cmp(i) as u64);
                }
                t
            });
        });
    });
    group.finish();
}

pub(super) fn bench_copy(c: &mut Criterion) {
    let cap = 256usize;
    // 随机生成cap个字节
    let slice = (0..cap)
        .into_iter()
        .map(|_| rand::random::<u8>())
        .collect::<Vec<u8>>();
    let len = slice.len();
    let runs = 64;
    let mut group = c.benchmark_group("ring_slice_copy");
    //let start = rand::random::<usize>();
    let start = 128;
    let rs = RingSlice::from(slice.as_ptr(), slice.len(), start, start + len);
    let mut dst = [0u8; 128];
    group.bench_function("copy_to_cmp", |b| {
        b.iter(|| {
            black_box({
                for i in 0..runs {
                    rs.copy_to_cmp(&mut dst[..], i, 64)
                }
            });
        });
    });
    group.bench_function("copy_to_r", |b| {
        b.iter(|| {
            black_box({
                for i in 0..runs {
                    rs.copy_to_r(&mut dst[..], i..i + 64);
                }
            });
        });
    });
    group.bench_function("copy_to_range", |b| {
        b.iter(|| {
            black_box({
                for i in 0..runs {
                    rs.copy_to_range(&mut dst[..], i..i + 64);
                }
            });
        });
    });
    group.finish();
}
