use criterion::{black_box, Criterion};
use ds::RingSlice;
use protocol::redis::Packet;
pub(super) fn parse(c: &mut Criterion) {
    let data = b"*2\r\n$5\r\nbfset\r\n$19\r\n9972602101111556910\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972601925349247790\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972602670110837550\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972603151400930094\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972603030906964782\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972602882608958254\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972602137802279726\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972601835448535854\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972602680699357998\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972601700260875054\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972602577506896686\r\n*2\r\n$5\r\nbfset\r\n$19\r\n9972601852848605998\r\n";
    let stream: RingSlice = data[..].into();
    let stream: Packet = stream.into();
    let mut group = c.benchmark_group("skip_bulks");
    //group.bench_function("num_skip_all", |b| {
    //    b.iter(|| {
    //        black_box({
    //            let mut oft = 0;
    //            while oft < stream.len() {
    //                stream.num_skip_all(&mut oft).expect("error not allowed");
    //            }
    //            assert_eq!(oft, stream.len());
    //        });
    //    });
    //});
    group.bench_function("skip_all_bulk", |b| {
        b.iter(|| {
            black_box({
                let mut oft = 0;
                while oft < stream.len() {
                    stream.skip_all_bulk(&mut oft).expect("error not allowed");
                }
                assert_eq!(oft, stream.len());
            });
        });
    });
    group.finish();
}

pub(super) fn parse_num(c: &mut Criterion) {
    let text = b"$2\r\n$-1$0\r\n$3\r\n$15\r\n$8\r\n$64\r\n$1\r\n$128\r\n$19\r\n$3\r\n$8\r\n$9\r\n$128\r\n$46\r\n$128\r\n";
    let data = b"*2\r\n*3\r\n*15\r\n*8\r\n*64\r\n*106\r\n*128\r\n*19\r\n*3\r\n*8\r\n*9\r\n*128\r\n*46\r\n*128\r\n*34\r\n";
    let stream: RingSlice = data[..].into();
    let text: RingSlice = text[..].into();
    let stream: Packet = stream.into();
    let text: Packet = text.into();
    let mut group = c.benchmark_group("parser_num");
    group.bench_function("num_of_bulks", |b| {
        b.iter(|| {
            black_box({
                let mut oft = 0;
                while oft < stream.len() {
                    stream.num_of_bulks(&mut oft).expect("error not allowed");
                }
                assert_eq!(oft, stream.len());
            });
        });
    });
    group.bench_function("num", |b| {
        b.iter(|| {
            black_box({
                let mut oft = 0;
                while oft < stream.len() {
                    stream.num(&mut oft).expect("error not allowed");
                }
                assert_eq!(oft, stream.len());
            });
        });
    });
    group.bench_function("string", |b| {
        b.iter(|| {
            black_box({
                let mut oft = 0;
                while oft < text.len() {
                    text.num_of_string(&mut oft).expect("error not allowed");
                }
                assert_eq!(oft, text.len());
            });
        });
    });
    group.finish();
}
