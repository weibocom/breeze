use criterion::{black_box, Criterion};
use ds::RingSlice;
use sharding::hash::{Crc32, Hash};
pub(super) fn bench_crc32(c: &mut Criterion) {
    let key = "ad_61fa9e8bc3624\r\nabcdjkfdajkd;aiejfjkd;ajflskdjfkkslajfiej";
    let idx = key.find('\r').expect("return not foudn");
    let key: RingSlice = key.as_bytes().into();
    let mut group = c.benchmark_group("hash");
    group.bench_function("crc32_basic", |b| {
        b.iter(|| {
            black_box({
                // 模拟redis先找到key再计算hash
                let idx = key.find_lf_cr(0).expect("no key found");
                let raw_key = key.sub_slice(0, idx);
                Crc32.hash(&raw_key)
            });
        });
    });
    group.bench_function("crc32_simple", |b| {
        b.iter(|| {
            black_box(Crc32.hash(&key.slice(0, idx)));
        });
    });
    group.finish();
}
