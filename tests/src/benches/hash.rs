use criterion::Criterion;
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
