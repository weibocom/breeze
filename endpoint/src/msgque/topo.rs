use discovery::TopologyWrite;
use protocol::{Builder, Protocol, Request, Resource};
use std::{
    collections::HashSet,
    ops::Bound::{Included, Unbounded},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::time::Instant;

use std::{
    collections::{BTreeMap, HashMap},
    time::Duration,
};

use protocol::{Endpoint, Topology};
use sharding::hash::{Hasher, HASH_PADDING};

// 读miss后的重试次数
const READ_RETRY_COUNT: usize = 5;
// 写失败后的重试次数
const WRITE_RETRY_COUNT: usize = 10;

#[derive(Clone)]
pub struct MsgQue<B, E, Req, P> {
    service: String,

    // 读写stream需要分开，读会有大量的空读
    streams_read: Vec<(String, E)>,
    streams_write: BTreeMap<usize, Vec<(String, E)>>,
    offline: Vec<(String, E, Instant)>,

    // 队列下一次应该要读取的位置索引(可能是当前的索引，也可能是下一个)
    read_idx_next: Arc<AtomicUsize>,
    // 在当前位置索引上读取消息的命中数量
    // read_idx_hits: Arc<usize>,

    // 在每个size的queue中，写的位置
    // write_cursors: HashMap<usize, usize>,
    parser: P,
    // 占位hasher，暂时不需要真实计算
    hash_padding: Hasher,
    max_size: usize,

    timeout_write: Duration,
    timeout_read: Duration,
    _marker: std::marker::PhantomData<(B, Req)>,
}

impl<B, E, Req, P> From<P> for MsgQue<B, E, Req, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            service: Default::default(),
            streams_read: Default::default(),
            streams_write: Default::default(),
            offline: Default::default(),
            read_idx_next: Arc::new(AtomicUsize::new(0)),
            // read_idx_hits: Arc::new(0),
            // write_cursors: Default::default(),
            parser: parser,
            max_size: super::BLOCK_SIZE,
            hash_padding: Hasher::from(HASH_PADDING),
            timeout_write: Duration::from_millis(200),
            timeout_read: Duration::from_millis(100),
            _marker: Default::default(),
        }
    }
}

impl<B, E, Req, P> discovery::Inited for MsgQue<B, E, Req, P>
where
    E: discovery::Inited,
{
    #[inline]
    fn inited(&self) -> bool {
        // check read streams
        let read_inited = self.streams_read.len() > 0
            && self
                .streams_read
                .iter()
                .fold(true, |inited, e| inited && e.1.inited());
        if !read_inited {
            return false;
        }

        // check write streams
        if self.streams_write.len() > 0 {
            for streams in self.streams_write.values() {
                for s in streams {
                    if !s.1.inited() {
                        return false;
                    }
                }
            }
            // 所有write streams 已初始化完毕，则为true
            return true;
        }
        return false;
    }
}

impl<B, E, Req, P> Topology for MsgQue<B, E, Req, P>
where
    B: Send + Sync,
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    #[inline]
    fn hasher(&self) -> &Hasher {
        &&self.hash_padding
    }
    #[inline]
    fn exp_sec(&self) -> u32 {
        log::error!("msg queue does't support expire");
        assert!(false);
        0
    }
}

//TODO: 验证的时候需要考虑512字节这种边界msg
impl<B, E, Req, P> protocol::Endpoint for MsgQue<B, E, Req, P>
where
    B: Send + Sync,
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    type Item = Req;
    #[inline]
    fn send(&self, mut req: Self::Item) {
        let mut ctx = super::Context::from(*req.mut_context());
        let inited = ctx.check_inited();
        let rw_count = ctx.get_and_incr_count();

        // 队列始终不需要write bakc
        req.write_back(false);

        // 对于读请求：顺序读取队列，如果队列都去了到数据，就连续读N个，如果没读到，则尝试下一个ip，直到轮询完所有的ip
        // 注意优先读取offline
        if req.operation().is_retrival() {
            // 避免处理req时，topo变更了
            let mut idx = self.read_idx_next.fetch_add(1, Ordering::Relaxed);
            if idx >= 10000000 {
                let _ = self.read_idx_next.compare_exchange(
                    idx,
                    0,
                    Ordering::Acquire,
                    Ordering::Relaxed,
                );
            }
            idx = idx % self.streams_read.len();

            // 计算重试次数，如果还有未读完的继续，否则停止try
            req.try_next(rw_count < READ_RETRY_COUNT);
            *req.mut_context() = ctx.ctx;
            log::debug!(
                "+++ mcq {} get from idx/{} req: {:?}",
                rw_count,
                idx,
                req.data()
            );
            self.streams_read.get(idx).unwrap().1.send(req);
            return;
        }

        // 写请求，需要根据消息的长度进行分组轮询写入
        let len = req.data().len();
        if !inited {
            // 该条msg第一次写队列
            for (i, streams) in self.streams_write.range((Included(len), Unbounded)) {
                // let widx = *self.write_cursors.get(i).unwrap_or(&0);
                let widx = rand::random::<usize>() % streams.len();
                // 修改mq topo下一次用的写索引，每次都用当前size que的下一个位置
                assert!(streams.len() > 0);
                // self.update_next_write_idx(*i, widx + 1);
                // req ctx 设置写的size、idx
                ctx.update_write_size(*i);
                ctx.update_idx(widx as u16);
                req.try_next(WRITE_RETRY_COUNT > 1);

                *req.mut_context() = ctx.ctx;

                log::debug!("+++ mcq set idx/{} req: {:?}", widx, req.data());

                // 发送写请求
                streams.get(widx).unwrap().1.send(req);
                return;
            }

            // 如果没有适合的queue，尝试用最大的
            req.try_next(false);
            log::warn!(
                "msgque {} - too big msg/{}: {:?}",
                self.service,
                len,
                req.data()
            );

            *req.mut_context() = ctx.ctx;
            self.streams_write
                .get(&self.max_size)
                .unwrap()
                .get(0)
                .unwrap()
                .1
                .send(req);
            return;
        } else {
            // 如果已经尝试写入，但失败，首先尝试本size的queue，如果本size的queue重试完毕，则继续重试更大size的queue
            let writed_size = ctx.get_write_size();
            let start_idx = ctx.get_idx();
            // 找到前一次写的streams
            if let Some(writed_streams) = self.streams_write.get(&writed_size) {
                if rw_count >= writed_streams.len() {
                    // TODO：如果某个queuesize全部写失败，先简化处理，后续改为继续写下一个size的queue？fishermen
                    req.try_next(false);
                } else {
                    req.try_next(rw_count < WRITE_RETRY_COUNT);
                }
                let i = (start_idx + rw_count) % writed_streams.len();

                *req.mut_context() = ctx.ctx;

                writed_streams.get(i).unwrap().1.send(req);
            } else {
                log::warn!(
                    "+++ msg too big so try the biggest stream:{}/{:?}",
                    req.len(),
                    req.data()
                );
                self.streams_write
                    .get(&self.max_size)
                    .unwrap()
                    .get(0)
                    .unwrap()
                    .1
                    .send(req);
            }
        }
    }
}

impl<B, E, Req, P> MsgQue<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    E: Endpoint<Item = Req>,
    P: Protocol,
{
    // 构建下线的队列
    fn build_offline(
        &mut self,
        sized_queue: &BTreeMap<usize, Vec<String>>,
    ) -> Vec<(String, E, Instant)> {
        let mut new_addrs = HashSet::with_capacity(self.streams_read.len());
        let _ = sized_queue
            .iter()
            .map(|(_, ss)| new_addrs.extend(ss.clone()));
        for (name, _) in self.streams_read.iter() {
            if !new_addrs.contains(name) {
                self.offline.push((
                    name.clone(),
                    B::build(
                        name,
                        self.parser.clone(),
                        Resource::MsgQue,
                        name,
                        self.timeout_read,
                    ),
                    Instant::now(),
                ));
            }
        }

        // TODO: 如果offline中有ip重新上线，清理掉
        let mut offline = Vec::with_capacity(self.offline.len());
        let old = self.offline.split_off(0);
        for (name, s, instance) in old.into_iter() {
            if !new_addrs.contains(&name) {
                offline.push((name, s, instance));
            }
        }

        offline
    }

    fn build_read_streams(
        &self,
        old: &mut HashMap<String, E>,
        addrs: &BTreeMap<usize, Vec<String>>,
        name: &str,
        timeout: Duration,
    ) -> Vec<(String, E)> {
        let mut streams = Vec::with_capacity(addrs.len());
        for ss in addrs.values() {
            streams.extend(ss);
        }
        streams
            .into_iter()
            .map(|s| {
                (
                    s.clone(),
                    old.remove(s).unwrap_or(B::build(
                        s,
                        self.parser.clone(),
                        Resource::MsgQue,
                        name,
                        timeout,
                    )),
                )
            })
            .collect()
    }

    fn build_write_stream(
        &mut self,
        addrs: &BTreeMap<usize, Vec<String>>,
        name: &str,
        timeout: Duration,
    ) -> BTreeMap<usize, Vec<(String, E)>> {
        let mut old_streams = HashMap::with_capacity(self.streams_write.len() * 3);
        if self.streams_write.len() > 0 {
            let mut first_key = 512;
            for (k, _) in self.streams_write.iter() {
                first_key = *k;
                break;
            }
            let old = self.streams_write.split_off(&first_key);
            for (_, ss) in old.into_iter() {
                for (addr, ep) in ss {
                    old_streams.insert(addr, ep);
                }
            }
        }

        let mut streams = BTreeMap::new();
        for (len, ss) in addrs.iter() {
            streams.insert(
                *len,
                ss.iter()
                    .map(|addr| {
                        (
                            addr.clone(),
                            old_streams.remove(addr).unwrap_or(B::build(
                                addr,
                                self.parser.clone(),
                                Resource::MsgQue,
                                name,
                                timeout,
                            )),
                        )
                    })
                    .collect(),
            );
        }

        // 轮询设置最大的que size
        let mut max_size = 0;
        for i in streams.keys() {
            if *i > max_size {
                max_size = *i;
            }
        }
        self.max_size = max_size;
        streams
    }
}

impl<B, E, Req, P> TopologyWrite for MsgQue<B, E, Req, P>
where
    B: Builder<P, Req, E>,
    P: Protocol,
    E: Endpoint<Item = Req>,
{
    #[inline]
    fn update(&mut self, name: &str, cfg: &str) {
        if let Some(ns) = super::config::Namespace::try_from(cfg, name) {
            log::debug!("+++ updating msgque for {}", name);
            assert!(ns.sized_queue.len() > 0);

            self.service = name.to_string();

            self.timeout_read = ns.timeout_read();
            self.timeout_write = ns.timeout_write();

            let old_r = self.streams_read.split_off(0);
            let mut old_streams_read: HashMap<String, E> =
                old_r.into_iter().map(|(addr, e)| (addr, e)).collect();

            // 首先构建offline stream
            self.offline = self.build_offline(&ns.sized_queue);

            // 构建读stream
            self.streams_read = self.build_read_streams(
                &mut old_streams_read,
                &ns.sized_queue,
                name,
                self.timeout_read,
            );

            // 构建写stream
            self.streams_write = self.build_write_stream(&ns.sized_queue, name, self.timeout_write);

            log::debug!(
                "+++ updated msgque for offline/{}, reads/{}, writes/{}",
                self.offline.len(),
                self.streams_read.len(),
                self.streams_write.len()
            );
        }
    }
}
