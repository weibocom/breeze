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
        let fristn = &self.inner[self.oft..self.oft + n];
        self.oft += n;
        ds::MemGuard::from_vec(fristn.to_vec())
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
