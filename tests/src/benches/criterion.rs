use criterion::{criterion_group, criterion_main};

mod arena;
mod hash;
mod heap;
mod redis;
mod ring_slice;
mod time;

criterion_group!(time, time::bench_instant, time::bench_duration);
criterion_group!(heap, heap::bench_get_checked);
criterion_group!(hash, hash::bench_crc32, hash::bench_num_to_str);
criterion_group!(redis, redis::parse, redis::parse_num);
criterion_group!(
    ring_slice,
    ring_slice::bench_iter,
    ring_slice::bench_read_num
);
criterion_group!(arena, arena::bench_alloc);

criterion_main!(time, heap, ring_slice, arena, hash, redis);
