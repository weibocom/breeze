use criterion::{criterion_group, criterion_main};
mod heap;
mod ring_slice;
mod time;

criterion_group!(time, time::bench_instant, time::bench_duration);
criterion_group!(heap, heap::bench_get_checked);
criterion_group!(ring_slice, ring_slice::bench_iter);

criterion_main!(time, heap, ring_slice);
