use criterion::{criterion_group, criterion_main};

mod arena;
mod hash;
mod heap;
mod kv;
mod redis;
mod ring_slice;
mod std_cmp;
mod time;

criterion_group!(std_cmp, std_cmp::bench_num_to_str, std_cmp::bench_mod);
criterion_group!(time, time::bench_instant, time::bench_duration);
criterion_group!(heap, heap::bench_get_checked);
criterion_group!(hash, hash::bench_crc32);
criterion_group!(redis, redis::parse, redis::parse_num);
criterion_group!(kv, kv::bench_parse_mysql);
criterion_group!(
    ring_slice,
    ring_slice::bench_iter,
    ring_slice::bench_copy,
    ring_slice::bench_read_num,
    ring_slice::bench_read_num_vs_start_with
);
criterion_group!(arena, arena::bench_alloc);

criterion_main!(time, heap, ring_slice, arena, hash, redis, std_cmp);
