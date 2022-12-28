use criterion::{criterion_group, criterion_main};
mod heap;
mod time;

use heap::*;
use time::*;

criterion_group!(time, bench_instant, bench_duration);
criterion_group!(heap, bench_get_checked);
criterion_main!(time, heap);
