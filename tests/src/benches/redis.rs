use criterion::{Criterion, black_box};
use ds::{BufWriter, RingSlice};
use protocol::{
    BufRead,
    redis::{Packet, Redis},
};
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
    // group.bench_function("skip_all_bulk", |b| {
    //     b.iter(|| {
    //         black_box({
    //             let mut oft = 0;
    //             while oft < stream.len() {
    //                 stream.skip_all_bulk(&mut oft).expect("error not allowed");
    //             }
    //             assert_eq!(oft, stream.len());
    //         });
    //     });
    // });
    let mut ctx = protocol::redis::ResponseContext {
        oft: 0,
        bulk: 0,
        status: protocol::redis::HandShakeStatus::Init,
    };
    group.bench_function("skip_multibulks", |b| {
        b.iter(|| {
            black_box({
                while ctx.oft < stream.len() {
                    stream
                        .skip_multibulks_with_ctx(&mut ctx)
                        .expect("error not allowed");
                }
                assert_eq!(ctx.oft, stream.len());
            });
        });
    });

    group.finish();
}

pub(super) fn parse_num(c: &mut Criterion) {
    // let text = b"$2\r\n$-1$0\r\n$3\r\n$15\r\n$8\r\n$64\r\n$1\r\n$128\r\n$19\r\n$3\r\n$8\r\n$9\r\n$128\r\n$46\r\n$128\r\n";
    let data = b"*2\r\n*3\r\n*15\r\n*8\r\n*64\r\n*106\r\n*128\r\n*19\r\n*3\r\n*8\r\n*9\r\n*128\r\n*46\r\n*128\r\n*34\r\n";
    let stream: RingSlice = data[..].into();
    // let text: RingSlice = text[..].into();
    let stream: Packet = stream.into();
    // let text: Packet = text.into();
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
    // group.bench_function("string", |b| {
    //     b.iter(|| {
    //         black_box({
    //             let mut oft = 0;
    //             while oft < text.len() {
    //                 text.num_of_string(&mut oft).expect("error not allowed");
    //             }
    //             assert_eq!(oft, text.len());
    //         });
    //     });
    // });
    group.finish();
}

#[allow(dead_code)]
mod proto_hook {
    use ds::BufWriter;
    use protocol::StreamContext;
    use sharding::hash::HashKey;

    use sharding::hash::Hash;

    use protocol::RequestProcessor;

    use protocol::HashedCommand;

    use protocol::Stream;

    use protocol::Writer;

    use protocol::BufRead;

    use protocol::AsyncBufRead;
    use protocol::Commander;

    use protocol::Metric;

    use std::cell::UnsafeCell;

    pub(crate) struct TestCtx {
        pub(crate) req: HashedCommand,
        pub(crate) metric: TestMetric,
    }

    impl TestCtx {
        pub(crate) fn new(req: HashedCommand) -> Self {
            Self {
                req,
                metric: TestMetric {
                    item: UnsafeCell::new(TestMetricItem {}),
                },
            }
        }
    }

    pub(crate) struct TestMetricItem {}

    impl std::ops::AddAssign<i64> for TestMetricItem {
        fn add_assign(&mut self, _rhs: i64) {}
    }

    impl std::ops::AddAssign<bool> for TestMetricItem {
        fn add_assign(&mut self, _rhs: bool) {}
    }

    pub(crate) struct TestMetric {
        pub(crate) item: UnsafeCell<TestMetricItem>,
    }

    impl Metric<TestMetricItem> for TestMetric {
        fn get(&self, _name: protocol::MetricName) -> &mut TestMetricItem {
            unsafe { &mut *self.item.get() }
        }
    }

    impl Commander<TestMetric, TestMetricItem> for TestCtx {
        fn request_mut(&mut self) -> &mut HashedCommand {
            todo!()
        }

        fn request(&self) -> &HashedCommand {
            &self.req
        }

        fn request_shard(&self) -> usize {
            todo!()
        }

        fn metric(&self) -> &TestMetric {
            &self.metric
        }

        fn ctx(&self) -> u64 {
            todo!()
        }
    }
    #[derive(Debug)]
    pub(crate) struct TestStream {
        pub(crate) oft: usize,
        pub(crate) inner: Vec<u8>,
        pub(crate) ctx: StreamContext,
    }

    impl AsyncBufRead for TestStream {
        fn poll_recv(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<protocol::Result<()>> {
            todo!()
        }
    }

    impl BufRead for TestStream {
        fn len(&self) -> usize {
            self.inner.len() - self.oft
        }

        fn slice(&self) -> ds::RingSlice {
            ds::RingSlice::from_slice(&self.inner[self.oft..])
        }

        fn take(&mut self, n: usize) -> ds::MemGuard {
            self.oft += n;
            ds::MemGuard::empty()
        }

        fn context(&mut self) -> &mut protocol::StreamContext {
            &mut self.ctx
        }

        fn reserve(&mut self, r: usize) {
            self.inner.reserve(r)
        }
    }

    impl BufWriter for TestStream {
        fn write_all(&mut self, _buf: &[u8]) -> std::io::Result<()> {
            self.inner.extend_from_slice(_buf);
            Ok(())
        }
    }

    impl Writer for TestStream {
        fn cap(&self) -> usize {
            todo!()
        }

        fn pending(&self) -> usize {
            todo!()
        }

        fn write(&mut self, data: &[u8]) -> protocol::Result<()> {
            self.inner.extend_from_slice(data);
            Ok(())
        }

        fn cache(&mut self, _hint: bool) {
            todo!()
        }

        fn shrink(&mut self) {
            todo!()
        }

        fn try_gc(&mut self) -> bool {
            todo!()
        }
    }

    impl Stream for TestStream {}

    pub(crate) struct Process {
        pub(crate) reqs: Vec<HashedCommand>,
    }

    impl RequestProcessor for Process {
        fn process(&mut self, req: HashedCommand, last: bool) {
            self.reqs.push(req);
            assert!(last)
        }
    }

    pub(crate) struct Alg {}

    impl Hash for Alg {
        fn hash<S: HashKey>(&self, _key: &S) -> i64 {
            0
        }
    }
}

pub(super) fn parse_proto(c: &mut Criterion) {
    let proto = Redis;
    let mut stream = proto_hook::TestStream {
        oft: 0,
        inner: Vec::new(),
        ctx: Default::default(),
    };
    let text = b"$6\r\nfoobar\r\n$6\r\nfoobar\r\n";
    stream.write_all(text).unwrap();
    let mut group = c.benchmark_group("parse_proto");
    group.bench_function("string", |b| {
        b.iter(|| {
            black_box({
                while stream.len() > 0 {
                    let _ = proto.parse_response_inner(&mut stream);
                }
                assert_eq!(stream.oft, text.len());
                stream.oft = 0;
            });
        });
    });

    let mut stream = proto_hook::TestStream {
        oft: 0,
        inner: Vec::new(),
        ctx: Default::default(),
    };
    let text = b"$8\r\nfield_97\r\n$8\r\nvalue_97\r\n$9\r\nfield_507\r\n$9\r\nvalue_507\r\n$10\r\nfield_1179\r\n$10\r\nvalue_1179\r\n$10\r\nfield_1774\r\n$10\r\nvalue_1774\r\n$10\r\nfield_1673\r\n$10\r\nvalue_1673\r\n$10\r\nfield_1317\r\n$10\r\nvalue_1317\r\n$9\r\nfield_787\r\n$9\r\nvalue_787\r\n$10\r\nfield_1869\r\n$10\r\nvalue_1869\r\n$9\r\nfield_664\r\n$9\r\nvalue_664\r\n$10\r\nfield_1839\r\n$10\r\nvalue_1839\r\n$10\r\nfield_1822\r\n$10\r\nvalue_1822\r\n$9\r\nfield_397\r\n$9\r\nvalue_397\r\n$8\r\nfield_83\r\n$8\r\nvalue_83\r\n$10\r\nfield_1675\r\n$10\r\nvalue_1675\r\n$9\r\nfield_720\r\n$9\r\nvalue_720\r\n$9\r\nfield_671\r\n$9\r\nvalue_671\r\n$9\r\nfield_710\r\n$9\r\nvalue_710\r\n$10\r\nfield_1962\r\n$10\r\nvalue_1962\r\n$10\r\nfield_1995\r\n$10\r\nvalue_1995\r\n$9\r\nfield_480\r\n$9\r\nvalue_480\r\n$9\r\nfield_330\r\n$9\r\nvalue_330\r\n$9\r\nfield_866\r\n$9\r\nvalue_866\r\n$10\r\nfield_1238\r\n$10\r\nvalue_1238\r\n$10\r\nfield_1053\r\n$10\r\nvalue_1053\r\n$9\r\nfield_513\r\n$9\r\nvalue_513\r\n$10\r\nfield_1259\r\n$10\r\nvalue_1259\r\n$10\r\nfield_1133\r\n$10\r\nvalue_1133\r\n$9\r\nfield_611\r\n$9\r\nvalue_611\r\n$9\r\nfield_956\r\n$9\r\nvalue_956\r\n$10\r\nfield_1651\r\n$10\r\nvalue_1651\r\n$9\r\nfield_386\r\n$9\r\nvalue_386\r\n$10\r\nfield_1700\r\n$10\r\nvalue_1700\r\n$10\r\nfield_1278\r\n$10\r\nvalue_1278\r\n$9\r\nfield_736\r\n$9\r\nvalue_736\r\n$10\r\nfield_1496\r\n$10\r\nvalue_1496\r\n$10\r\nfield_1255\r\n$9\r\nvalue_125\r\n";
    stream.write_all(text).unwrap();
    group.bench_function("multi_string", |b| {
        b.iter(|| {
            black_box({
                while stream.len() > 0 {
                    let _ = proto.parse_response_inner(&mut stream);
                }
                assert_eq!(stream.oft, text.len());
                stream.oft = 0;
            });
        });
    });

    let mut stream = proto_hook::TestStream {
        oft: 0,
        inner: Vec::new(),
        ctx: Default::default(),
    };
    let text = b"*2\r\n*3\r\n$7\r\nBeijing\r\n$8\r\n132.6218\r\n*2\r\n$20\r\n30.00000089406967163\r\n$20\r\n49.99999957172130394\r\n*3\r\n$7\r\nTianjin\r\n$8\r\n573.0514\r\n*2\r\n$20\r\n26.99999839067459106\r\n$20\r\n53.99999994301438733\r\n";
    stream.write_all(text).unwrap();
    group.bench_function("bulk", |b| {
        b.iter(|| {
            black_box({
                let _ = proto.parse_response_inner(&mut stream);
                assert_eq!(stream.oft, text.len());
                stream.oft = 0;
            });
        });
    });

    let mut stream = proto_hook::TestStream {
        oft: 0,
        inner: Vec::new(),
        ctx: Default::default(),
    };
    let text = b"*232\r\n$9\r\nfield_256\r\n$7\r\nval_256\r\n$9\r\nfield_257\r\n$7\r\nval_257\r\n$9\r\nfield_258\r\n$7\r\nval_258\r\n$9\r\nfield_259\r\n$7\r\nval_259\r\n$9\r\nfield_260\r\n$7\r\nval_260\r\n$9\r\nfield_261\r\n$7\r\nval_261\r\n$9\r\nfield_262\r\n$7\r\nval_262\r\n$9\r\nfield_263\r\n$7\r\nval_263\r\n$9\r\nfield_264\r\n$7\r\nval_264\r\n$9\r\nfield_265\r\n$7\r\nval_265\r\n$9\r\nfield_266\r\n$7\r\nval_266\r\n$9\r\nfield_267\r\n$7\r\nval_267\r\n$9\r\nfield_268\r\n$7\r\nval_268\r\n$9\r\nfield_269\r\n$7\r\nval_269\r\n$9\r\nfield_270\r\n$7\r\nval_270\r\n$9\r\nfield_271\r\n$7\r\nval_271\r\n$9\r\nfield_272\r\n$7\r\nval_272\r\n$9\r\nfield_273\r\n$7\r\nval_273\r\n$9\r\nfield_274\r\n$7\r\nval_274\r\n$9\r\nfield_275\r\n$7\r\nval_275\r\n$9\r\nfield_276\r\n$7\r\nval_276\r\n$9\r\nfield_277\r\n$7\r\nval_277\r\n$9\r\nfield_278\r\n$7\r\nval_278\r\n$9\r\nfield_279\r\n$7\r\nval_279\r\n$9\r\nfield_280\r\n$7\r\nval_280\r\n$9\r\nfield_281\r\n$7\r\nval_281\r\n$9\r\nfield_282\r\n$7\r\nval_282\r\n$9\r\nfield_283\r\n$7\r\nval_283\r\n$9\r\nfield_284\r\n$7\r\nval_284\r\n$9\r\nfield_285\r\n$7\r\nval_285\r\n$9\r\nfield_286\r\n$7\r\nval_286\r\n$9\r\nfield_287\r\n$7\r\nval_287\r\n$9\r\nfield_288\r\n$7\r\nval_288\r\n$9\r\nfield_289\r\n$7\r\nval_289\r\n$9\r\nfield_290\r\n$7\r\nval_290\r\n$9\r\nfield_291\r\n$7\r\nval_291\r\n$9\r\nfield_292\r\n$7\r\nval_292\r\n$9\r\nfield_293\r\n$7\r\nval_293\r\n$9\r\nfield_294\r\n$7\r\nval_294\r\n$9\r\nfield_295\r\n$7\r\nval_295\r\n$9\r\nfield_296\r\n$7\r\nval_296\r\n$9\r\nfield_297\r\n$7\r\nval_297\r\n$9\r\nfield_298\r\n$7\r\nval_298\r\n$9\r\nfield_299\r\n$7\r\nval_299\r\n$9\r\nfield_300\r\n$7\r\nval_300\r\n$9\r\nfield_301\r\n$7\r\nval_301\r\n$9\r\nfield_302\r\n$7\r\nval_302\r\n$9\r\nfield_303\r\n$7\r\nval_303\r\n$9\r\nfield_304\r\n$7\r\nval_304\r\n$9\r\nfield_305\r\n$7\r\nval_305\r\n$9\r\nfield_306\r\n$7\r\nval_306\r\n$9\r\nfield_307\r\n$7\r\nval_307\r\n$9\r\nfield_308\r\n$7\r\nval_308\r\n$9\r\nfield_309\r\n$7\r\nval_309\r\n$9\r\nfield_310\r\n$7\r\nval_310\r\n$9\r\nfield_311\r\n$7\r\nval_311\r\n$9\r\nfield_312\r\n$7\r\nval_312\r\n$9\r\nfield_313\r\n$7\r\nval_313\r\n$9\r\nfield_314\r\n$7\r\nval_314\r\n$9\r\nfield_315\r\n$7\r\nval_315\r\n$9\r\nfield_316\r\n$7\r\nval_316\r\n$9\r\nfield_317\r\n$7\r\nval_317\r\n$9\r\nfield_318\r\n$7\r\nval_318\r\n$9\r\nfield_319\r\n$7\r\nval_319\r\n$9\r\nfield_320\r\n$7\r\nval_320\r\n$9\r\nfield_321\r\n$7\r\nval_321\r\n$9\r\nfield_322\r\n$7\r\nval_322\r\n$9\r\nfield_323\r\n$7\r\nval_323\r\n$9\r\nfield_324\r\n$7\r\nval_324\r\n$9\r\nfield_325\r\n$7\r\nval_325\r\n$9\r\nfield_326\r\n$7\r\nval_326\r\n$9\r\nfield_327\r\n$7\r\nval_327\r\n$9\r\nfield_328\r\n$7\r\nval_328\r\n$9\r\nfield_329\r\n$7\r\nval_329\r\n$9\r\nfield_330\r\n$7\r\nval_330\r\n$9\r\nfield_331\r\n$7\r\nval_331\r\n$9\r\nfield_332\r\n$7\r\nval_332\r\n$9\r\nfield_333\r\n$7\r\nval_333\r\n$9\r\nfield_334\r\n$7\r\nval_334\r\n$9\r\nfield_335\r\n$7\r\nval_335\r\n$9\r\nfield_336\r\n$7\r\nval_336\r\n$9\r\nfield_337\r\n$7\r\nval_337\r\n$9\r\nfield_338\r\n$7\r\nval_338\r\n$9\r\nfield_339\r\n$7\r\nval_339\r\n$9\r\nfield_340\r\n$7\r\nval_340\r\n$9\r\nfield_341\r\n$7\r\nval_341\r\n$9\r\nfield_342\r\n$7\r\nval_342\r\n$9\r\nfield_343\r\n$7\r\nval_343\r\n$9\r\nfield_344\r\n$7\r\nval_344\r\n$9\r\nfield_345\r\n$7\r\nval_345\r\n$9\r\nfield_346\r\n$7\r\nval_346\r\n$9\r\nfield_347\r\n$7\r\nval_347\r\n$9\r\nfield_348\r\n$7\r\nval_348\r\n$9\r\nfield_349\r\n$7\r\nval_349\r\n$9\r\nfield_350\r\n$7\r\nval_350\r\n$9\r\nfield_351\r\n$7\r\nval_351\r\n$9\r\nfield_352\r\n$7\r\nval_352\r\n$9\r\nfield_353\r\n$7\r\nval_353\r\n$9\r\nfield_354\r\n$7\r\nval_354\r\n$9\r\nfield_355\r\n$7\r\nval_355\r\n$9\r\nfield_356\r\n$7\r\nval_356\r\n$9\r\nfield_357\r\n$7\r\nval_357\r\n$9\r\nfield_358\r\n$7\r\nval_358\r\n$9\r\nfield_359\r\n$7\r\nval_359\r\n$9\r\nfield_360\r\n$7\r\nval_360\r\n$9\r\nfield_361\r\n$7\r\nval_361\r\n$9\r\nfield_362\r\n$7\r\nval_362\r\n$9\r\nfield_363\r\n$7\r\nval_363\r\n$9\r\nfield_364\r\n$7\r\nval_364\r\n$9\r\nfield_365\r\n$7\r\nval_365\r\n$9\r\nfield_366\r\n$7\r\nval_366\r\n$9\r\nfield_367\r\n$7\r\nval_367\r\n$9\r\nfield_368\r\n$7\r\nval_368\r\n$9\r\nfield_369\r\n$7\r\nval_369\r\n$9\r\nfield_370\r\n$7\r\nval_370\r\n$9\r\nfield_371\r\n$7\r\nval_371\r\n";
    stream.write_all(text).unwrap();
    group.bench_function("long_bulk", |b| {
        b.iter(|| {
            black_box({
                let _ = proto.parse_response_inner(&mut stream);
                assert_eq!(stream.oft, text.len());
                stream.oft = 0;
            });
        });
    });

    let mut stream = proto_hook::TestStream {
        oft: 0,
        inner: Vec::new(),
        ctx: Default::default(),
    };
    let text = b"*7\r\n$3\r\nfoo\r\n$-1\r\n:1\r\n+Foo\r\n+Bar\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n+Bar\r\n";
    stream.write_all(text).unwrap();
    group.bench_function("mix", |b| {
        b.iter(|| {
            black_box({
                let _ = proto.parse_response_inner(&mut stream);
                assert_eq!(stream.oft, text.len());
                stream.oft = 0;
            });
        });
    });
    group.finish();
}
