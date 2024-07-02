use std::cell::UnsafeCell;

use ds::BufWriter;
use protocol::{
    msgque::{McqText, OP_GET, OP_QUIT, OP_SET, OP_STATS, OP_VERSION},
    AsyncBufRead, Attachment, BufRead, Commander, Error, HashedCommand, Metric, Proto,
    RequestProcessor, Stream, Writer,
};
use sharding::hash::{Hash, HashKey};

#[derive(Debug)]
struct VecStream {
    oft: usize,
    inner: Vec<u8>,
}

impl AsyncBufRead for VecStream {
    fn poll_recv(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<protocol::Result<()>> {
        todo!()
    }
}

impl BufRead for VecStream {
    fn len(&self) -> usize {
        self.inner.len() - self.oft
    }

    fn slice(&self) -> ds::RingSlice {
        ds::RingSlice::from_vec(&self.inner)
    }

    fn take(&mut self, n: usize) -> ds::MemGuard {
        let fristn = &self.inner[self.oft..self.oft + n];
        self.oft += n;
        ds::MemGuard::from_vec(fristn.to_vec())
    }

    fn context(&mut self) -> &mut protocol::StreamContext {
        todo!()
    }

    fn reserve(&mut self, r: usize) {
        self.inner.reserve(r)
    }
}

impl BufWriter for VecStream {
    fn write_all(&mut self, _buf: &[u8]) -> std::io::Result<()> {
        self.inner.extend_from_slice(_buf);
        Ok(())
    }
}

impl Writer for VecStream {
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

impl Stream for VecStream {}

struct Process {
    reqs: Vec<HashedCommand>,
}

impl RequestProcessor for Process {
    fn process(&mut self, req: HashedCommand, last: bool) {
        self.reqs.push(req);
        assert!(last)
    }
}

struct Alg {}
impl Hash for Alg {
    fn hash<S: HashKey>(&self, _key: &S) -> i64 {
        0
    }
}

/// 请求以任意长度发送
#[test]
fn test_req_reenter() {
    let getset = b"get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\nset key4 0 9999 10\r\n1234567890\r\nget key1\r\nget key2\r\n";

    let proto = McqText;
    let alg = &Alg {};
    for i in 0..getset.len() {
        let mut process = Process { reqs: Vec::new() };
        let (req1, _) = getset.split_at(i);
        let mut stream = VecStream {
            oft: 0,
            inner: req1.to_vec(),
        };
        let _ = proto.parse_request(&mut stream, alg, &mut process);
        //解析的请求和req1中的请求一致
        if i < "get key1\r\n".len() {
            assert_eq!(process.reqs.len(), 0);
            assert_eq!(stream.len(), req1.len());
        }
        if "get key1\r\n".len() <= i && i < "get key1\r\nget key2\r\n".len() {
            assert_eq!(process.reqs.len(), 1);
            let req = &process.reqs[0];
            assert_eq!(req.op_code(), OP_GET);
            assert_eq!(req.noforward(), false);
            assert!(req.equal(b"get key1\r\n"));
            assert_eq!(stream.len(), req1.len() - "get key1\r\n".len());
        }
        if "get key1\r\nget key2\r\n".len() <= i
            && i < "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\n".len()
        {
            assert_eq!(process.reqs.len(), 2, "{}: {:?}", i, process.reqs);
            let req = &process.reqs[1];
            assert_eq!(req.op_code(), OP_GET);
            assert_eq!(req.noforward(), false);
            assert!(req.equal(b"get key2\r\n"), "{:?}", process.reqs);
            assert_eq!(stream.len(), req1.len() - "get key1\r\nget key2\r\n".len());
        }
        if "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\n".len() <= i && 
        i < "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\nset key4 0 9999 10\r\n1234567890\r\n".len() {
            assert_eq!(process.reqs.len(), 3);
            let req = &process.reqs[2];
            assert_eq!(req.op_code(), OP_SET);
            assert_eq!(req.noforward(), false);
            assert!(req.equal(b"set key3 0 9999 10\r\n1234567890\r\n"));
            assert_eq!(stream.len(), req1.len() - "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\n".len());
            //第二个请求也没问题
            let req = &process.reqs[1];
            assert_eq!(req.op_code(), OP_GET);
            assert_eq!(req.noforward(), false);
            assert!(req.equal(b"get key2\r\n"));
        }
        if "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\nset key4 0 9999 10\r\n1234567890\r\n".len() <= i && 
        i < "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\nset key4 0 9999 10\r\n1234567890\r\nget key1\r\n".len() {
            assert_eq!(process.reqs.len(), 4);
            let req = &process.reqs[3];
            assert_eq!(req.op_code(), OP_SET);
            assert_eq!(req.noforward(), false);
            assert!(req.equal(b"set key4 0 9999 10\r\n1234567890\r\n"));
            assert_eq!(stream.len(), req1.len() - "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\nset key4 0 9999 10\r\n1234567890\r\n".len());
        }
        if "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\nset key4 0 9999 10\r\n1234567890\r\nget key1\r\n".len() <= i &&
         i < "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\nset key4 0 9999 10\r\n1234567890\r\nget key1\r\nget key2\r\n".len() {
            assert_eq!(process.reqs.len(), 5);
            let req = &process.reqs[4];
            assert_eq!(req.op_code(), OP_GET);
            assert_eq!(req.noforward(), false);
            assert!(req.equal(b"get key1\r\n"));
            assert_eq!(stream.len(), req1.len() - "get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\nset key4 0 9999 10\r\n1234567890\r\nget key1\r\n".len());
        }
    }
}

#[test]
fn test_meta() {
    let proto = McqText;
    let alg = &Alg {};

    let mut process = Process { reqs: Vec::new() };
    let req_str = b"version\r\n";
    let mut stream = VecStream {
        oft: 0,
        inner: req_str.to_vec(),
    };
    let _ = proto.parse_request(&mut stream, alg, &mut process);
    assert_eq!(process.reqs.len(), 1);
    let req = &process.reqs[0];
    assert_eq!(req.op_code(), OP_VERSION);
    assert_eq!(req.noforward(), true);
    assert!(req.equal(req_str));
    assert_eq!(stream.len(), 0);

    let mut process = Process { reqs: Vec::new() };
    let req_str = b"quit\r\n";
    let mut stream = VecStream {
        oft: 0,
        inner: req_str.to_vec(),
    };
    let _ = proto.parse_request(&mut stream, alg, &mut process);
    assert_eq!(process.reqs.len(), 1);
    let req = &process.reqs[0];
    assert_eq!(req.op_code(), OP_QUIT);
    assert_eq!(req.noforward(), true);
    assert!(req.equal(req_str));
    assert_eq!(stream.len(), 0);

    let mut process = Process { reqs: Vec::new() };
    let req_str = b"stats\r\n";
    let mut stream = VecStream {
        oft: 0,
        inner: req_str.to_vec(),
    };
    let _ = proto.parse_request(&mut stream, alg, &mut process);
    assert_eq!(process.reqs.len(), 1);
    let req = &process.reqs[0];
    assert_eq!(req.op_code(), OP_STATS);
    assert_eq!(req.noforward(), true);
    assert!(req.equal(req_str));
    assert_eq!(stream.len(), 0);
}

#[test]
fn test_rsp() {
    let proto = McqText;

    let rspstr = b"END\r\n";
    for i in 0..rspstr.len() {
        let (rspstr1, _) = rspstr.split_at(i);
        let mut stream = VecStream {
            oft: 0,
            inner: rspstr1.to_vec(),
        };
        let Ok(rsp) = proto.parse_response(&mut stream) else {
            panic!("parse_response failed");
        };
        if i < rspstr.len() {
            assert!(rsp.is_none());
            assert_eq!(stream.len(), rspstr1.len());
        } else {
            assert!(rsp.is_some());
            let rsp = rsp.unwrap();
            assert!(rsp.equal(rspstr));
            assert!(!rsp.ok());
            assert_eq!(stream.len(), 0);
        }
    }

    let rspstr = b"VALUE key1 0 10\r\n1234567890\r\nEND\r\n";
    for i in 0..rspstr.len() {
        let (rspstr1, _) = rspstr.split_at(i);
        let mut stream = VecStream {
            oft: 0,
            inner: rspstr1.to_vec(),
        };
        let Ok(rsp) = proto.parse_response(&mut stream) else {
            panic!("parse_response failed");
        };
        if i < rspstr.len() {
            assert!(rsp.is_none());
            assert_eq!(stream.len(), rspstr1.len());
        } else {
            assert!(rsp.is_some());
            let rsp = rsp.unwrap();
            assert!(rsp.equal(rspstr));
            assert!(rsp.ok());
            assert_eq!(stream.len(), 0);
        }
    }

    let rspstr = b"STORED\r\n";
    for i in 0..rspstr.len() {
        let (rspstr1, _) = rspstr.split_at(i);
        let mut stream = VecStream {
            oft: 0,
            inner: rspstr1.to_vec(),
        };
        let Ok(rsp) = proto.parse_response(&mut stream) else {
            panic!("parse_response failed");
        };
        if i < rspstr.len() {
            assert!(rsp.is_none());
            assert_eq!(stream.len(), rspstr1.len());
        } else {
            assert!(rsp.is_some());
            let rsp = rsp.unwrap();
            assert!(rsp.equal(rspstr));
            assert!(rsp.ok());
            assert_eq!(stream.len(), 0);
        }
    }

    let rspstr = b"NOT_STORED\r\n";
    for i in 0..rspstr.len() {
        let (rspstr1, _) = rspstr.split_at(i);
        let mut stream = VecStream {
            oft: 0,
            inner: rspstr1.to_vec(),
        };
        let Ok(rsp) = proto.parse_response(&mut stream) else {
            panic!("parse_response failed");
        };
        if i < rspstr.len() {
            assert!(rsp.is_none());
            assert_eq!(stream.len(), rspstr1.len());
        } else {
            assert!(rsp.is_some());
            let rsp = rsp.unwrap();
            assert!(rsp.equal(rspstr));
            assert!(!rsp.ok());
            assert_eq!(stream.len(), 0);
        }
    }
}

struct TestCtx {
    req: HashedCommand,
    metric: TestMetric,
}

impl TestCtx {
    fn new(req: HashedCommand) -> Self {
        Self {
            req,
            metric: TestMetric {
                item: UnsafeCell::new(TestMetricItem {}),
            },
        }
    }
}

struct TestMetricItem {}
impl std::ops::AddAssign<i64> for TestMetricItem {
    fn add_assign(&mut self, _rhs: i64) {}
}
impl std::ops::AddAssign<bool> for TestMetricItem {
    fn add_assign(&mut self, _rhs: bool) {}
}

struct TestMetric {
    item: UnsafeCell<TestMetricItem>,
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

    fn attachment(&self) -> Option<&Attachment> {
        todo!()
    }
}

#[test]
fn test_write_response() {
    let proto = McqText;
    let alg = &Alg {};

    let mut process = Process { reqs: Vec::new() };
    let req_str = b"version\r\n";
    let mut stream = VecStream {
        oft: 0,
        inner: req_str.to_vec(),
    };
    let _ = proto.parse_request(&mut stream, alg, &mut process);
    assert_eq!(process.reqs.len(), 1);
    let req = process.reqs.into_iter().next().unwrap();
    let mut ctx = TestCtx::new(req);
    let mut stream = VecStream {
        oft: 0,
        inner: Vec::new(),
    };
    let _ = proto.write_response(&mut ctx, None, &mut stream);
    let resp_str = b"VERSION 0.0.1\r\n";
    assert_eq!(stream.inner, resp_str.to_vec());

    let mut process = Process { reqs: Vec::new() };
    let req_str = b"stats\r\n";
    let mut stream = VecStream {
        oft: 0,
        inner: req_str.to_vec(),
    };
    let _ = proto.parse_request(&mut stream, alg, &mut process);
    assert_eq!(process.reqs.len(), 1);
    let req = process.reqs.into_iter().next().unwrap();
    let mut ctx = TestCtx::new(req);
    let mut stream = VecStream {
        oft: 0,
        inner: Vec::new(),
    };
    let _ = proto.write_response(&mut ctx, None, &mut stream);
    let resp_str = b"STAT supported later\r\nEND\r\n";
    assert_eq!(stream.inner, resp_str.to_vec());

    let mut process = Process { reqs: Vec::new() };
    let req_str = b"quit\r\n";
    let mut stream = VecStream {
        oft: 0,
        inner: req_str.to_vec(),
    };
    let _ = proto.parse_request(&mut stream, alg, &mut process);
    assert_eq!(process.reqs.len(), 1);
    let req = process.reqs.into_iter().next().unwrap();
    let mut ctx = TestCtx::new(req);
    let mut stream = VecStream {
        oft: 0,
        inner: Vec::new(),
    };
    let e = proto.write_response(&mut ctx, None, &mut stream);
    let Err(Error::Quit) = e else {
        panic!("expected quit error")
    };

    let mut process = Process { reqs: Vec::new() };
    let req_str = b"get key1\r\n";
    let mut stream = VecStream {
        oft: 0,
        inner: req_str.to_vec(),
    };
    let _ = proto.parse_request(&mut stream, alg, &mut process);
    assert_eq!(process.reqs.len(), 1);
    let req = process.reqs.into_iter().next().unwrap();
    let mut ctx = TestCtx::new(req);
    let rspstr = b"VALUE key1 0 10\r\n1234567890\r\nEND\r\n";
    let mut rsp_stream = VecStream {
        oft: 0,
        inner: rspstr.to_vec(),
    };
    let Ok(mut rsp) = proto.parse_response(&mut rsp_stream) else {
        panic!("parse_response failed");
    };
    let mut stream = VecStream {
        oft: 0,
        inner: Vec::new(),
    };
    let _ = proto.write_response(&mut ctx, rsp.as_mut(), &mut stream);
    assert_eq!(stream.inner, rspstr.to_vec());

    //没有查到
    let mut process = Process { reqs: Vec::new() };
    let req_str = b"get key1\r\n";
    let mut stream = VecStream {
        oft: 0,
        inner: req_str.to_vec(),
    };
    let _ = proto.parse_request(&mut stream, alg, &mut process);
    assert_eq!(process.reqs.len(), 1);
    let req = process.reqs.into_iter().next().unwrap();
    let mut ctx = TestCtx::new(req);
    let rspstr = b"END\r\n";
    let mut rsp_stream = VecStream {
        oft: 0,
        inner: rspstr.to_vec(),
    };
    let Ok(mut rsp) = proto.parse_response(&mut rsp_stream) else {
        panic!("parse_response failed");
    };
    let mut stream = VecStream {
        oft: 0,
        inner: Vec::new(),
    };
    let _ = proto.write_response(&mut ctx, rsp.as_mut(), &mut stream);
    assert_eq!(stream.inner, rspstr.to_vec());

    //没有响应
    let mut process = Process { reqs: Vec::new() };
    let req_str = b"get key1\r\n";
    let mut stream = VecStream {
        oft: 0,
        inner: req_str.to_vec(),
    };
    let _ = proto.parse_request(&mut stream, alg, &mut process);
    assert_eq!(process.reqs.len(), 1);
    let req = process.reqs.into_iter().next().unwrap();
    let mut ctx = TestCtx::new(req);
    let mut stream = VecStream {
        oft: 0,
        inner: Vec::new(),
    };
    let _ = proto.write_response(&mut ctx, None, &mut stream);
    assert_eq!(stream.inner, b"SERVER_ERROR mcq not available\r\n".to_vec());
}
