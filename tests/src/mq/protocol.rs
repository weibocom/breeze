use ds::BufWriter;
use protocol::{
    msgque::{McqText, OP_GET},
    AsyncBufRead, BufRead, HashedCommand, Proto, RequestProcessor, Stream, Writer,
};
use sharding::hash::{Hash, HashKey};

#[derive(Debug)]
struct VecStream {
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
        self.inner.len()
    }

    fn slice(&self) -> ds::RingSlice {
        ds::RingSlice::from_vec(&self.inner)
    }

    fn take(&mut self, n: usize) -> ds::MemGuard {
        let fristn = self.inner.drain(..n).collect();
        ds::MemGuard::from_vec(fristn)
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
        todo!()
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

/// get分两个包解析
#[test]
fn test_reenter() {
    let getset = b"get key1\r\nget key2\r\nset key3 0 9999 10\r\n1234567890\r\nset key4 0 9999 10\r\n1234567890\r\nget key1\r\nget key2\r\n";

    let proto = McqText;
    let alg = &Alg {};
    for i in 0..getset.len() {
        let mut process = Process { reqs: Vec::new() };
        let (req1, _) = getset.split_at(i);
        let mut stream = VecStream {
            inner: req1.to_vec(),
        };
        proto.parse_request(&mut stream, alg, &mut process);
        //解析的请求和req1中的请求一致
        if 0 <= i && i < "get key1\r\n".len() {
            assert_eq!(process.reqs.len(), 0);
            assert_eq!(stream.inner.len(), req1.len());
        }
        if "get key1\r\n".len() <= i && i < "get key1\r\nget key2\r\n".len() {
            assert_eq!(process.reqs.len(), 1);
            let req = process.reqs[0];
            assert_eq!(req.op_code(), OP_GET);
            assert_eq!(req.noforward(), false);
            assert!(req.equal(b"get key1\r\n"));
            assert_eq!(stream.inner.len(), req1.len() - "get key1\r\n".len());
        }
    }
}
