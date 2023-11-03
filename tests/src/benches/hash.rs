use criterion::Criterion;
use ds::RingSlice;
use sharding::hash::{Crc32, Hash, Hasher};
pub(super) fn bench_crc32(c: &mut Criterion) {
    let key = "ad_61fa24jfdkd\r\nabcdjkfdajkd;aiejfjkd;ajflskdjfkkslajfiej";
    let idx = key.find('\r').expect("return not found");
    let rs: RingSlice = key.as_bytes().into();
    let mut group = c.benchmark_group("hash");
    let crc32 = Hasher::Crc32(Crc32);
    group.bench_function("crc32_basic", |b| {
        b.iter(|| crc32.hash(&rs.slice(0, idx)));
    });
    group.bench_function("crc32_simple", |b| {
        b.iter(|| Crc32.hash(&rs.slice(0, idx)));
    });
    let bkey = &key.as_bytes()[..idx];
    group.bench_function("crc32_raw", |b| {
        b.iter(|| Crc32.hash(&bkey));
    });
    let crc = sharding::hash::crc::Crc32::default();
    group.bench_function("crc32_hasher", |b| {
        b.iter(|| crc.hash(&bkey));
    });
    group.finish();
}
