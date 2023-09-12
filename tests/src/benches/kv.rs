use criterion::Criterion;
pub(super) fn bench_parse_mysql(c: &mut Criterion) {
    //let text = "1234-12-12 12:12:12.123456";
    let group = c.benchmark_group("parse_mysql");
    //group.bench_function("YYYY-MM-DD HH:MM:SS.MICRO", |b| {
    //    b.iter(|| {
    //        black_box({
    //            parse_mysql_datetime_string_with_time(text.as_bytes()).unwrap();
    //        });
    //    });
    //});
    //let text = "12:34:56.012345";
    //group.bench_function("HH:MM:SS.MICRO", |b| {
    //    b.iter(|| {
    //        black_box({
    //            parse_mysql_time_string_with_time(text.as_bytes()).unwrap();
    //        });
    //    });
    //});
    group.finish();
}
//#[cfg(feature = "nightly")]
//#[cfg(feature = "chrono")]
//#[bench]
//fn bench_parse_mysql_datetime_string(bencher: &mut test::Bencher) {
//    let text = "1234-12-12 12:12:12.123456";
//    bencher.bytes = text.len() as u64;
//    bencher.iter(|| {
//        parse_mysql_datetime_string(text.as_bytes()).unwrap();
//    });
//}
//
//#[cfg(feature = "nightly")]
//#[bench]
//fn bench_parse_mysql_time_string(bencher: &mut test::Bencher) {
//    let text = "-012:34:56.012345";
//    bencher.bytes = text.len() as u64;
//    bencher.iter(|| {
//        parse_mysql_time_string(text.as_bytes()).unwrap();
//    });
//}
