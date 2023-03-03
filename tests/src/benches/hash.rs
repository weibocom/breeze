use criterion::{black_box, Criterion};
use ds::RingSlice;
use sharding::hash::{Crc32, Hash, Hasher};
pub(super) fn bench_crc32(c: &mut Criterion) {
    let key = "ad_61fa24jfdkd\r\nabcdjkfdajkd;aiejfjkd;ajflskdjfkkslajfiej";
    let idx = key.find('\r').expect("return not foudn");
    let key: RingSlice = key.as_bytes().into();
    let mut group = c.benchmark_group("hash");
    let crc32 = Hasher::Crc32(Crc32);
    group.bench_function("crc32_basic", |b| {
        b.iter(|| {
            crc32.hash(&key.slice(0, idx))
            //black_box(crc32.hash(&key.slice(0, idx)));
        });
    });
    group.bench_function("crc32_simple", |b| {
        b.iter(|| {
            Crc32.hash(&key.slice(0, idx))
            //black_box(Crc32.hash(&key.slice(0, idx)));
        });
    });
    group.finish();
}
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
