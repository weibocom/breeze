use discovery::TopologyWrite;
use protocol::{Protocol, Request, Resource};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::time::Instant;

use std::collections::{BTreeMap, HashMap};

use crate::{Endpoint, Timeout, Topology};
use sharding::hash::{Hash, HashKey, Hasher, Padding};

use crate::msgque::strategy::hitfirst::Node;

use super::{
    strategy::{hitfirst::HitFirstReader, robbin::RobbinWriter, QID},
    Context,
};

// 读miss后的重试次数
const READ_RETRY_COUNT: usize = 3;
// 写失败后的重试次数
const WRITE_RETRY_COUNT: usize = 3;
// ip vintage下线后，N分钟后停止读
const OFFLINE_STOP_READ_SECONDS: u64 = 60 * 20;
// ip vintage下线后，N分钟从内存清理
const OFFLINE_CLEAN_SECONDS: u64 = OFFLINE_STOP_READ_SECONDS + 60 * 2;

#[derive(Clone)]
pub struct MsgQue<E, P> {
    service: String,

    // 读写stream需要分开，读会有大量的空读
    streams_read: Vec<(String, E, usize)>,
    streams_write: BTreeMap<usize, Vec<(String, E)>>,

    // 轮询访问，N分钟后下线
    streams_offline: Vec<(String, E)>,
    offline_hits: Arc<AtomicUsize>,
    offline_time: Instant,

    read_strategy: Arc<HitFirstReader>,
    write_strategy: Arc<RobbinWriter>,
    parser: P,
    // 占位hasher，暂时不需要真实计算
    max_size: usize,

    timeout_write: Timeout,
    timeout_read: Timeout,
}

impl<E, P> From<P> for MsgQue<E, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            service: Default::default(),
            streams_read: Default::default(),
            streams_write: Default::default(),
            streams_offline: Default::default(),
            offline_hits: Default::default(),
            offline_time: Instant::now(),
            read_strategy: Default::default(),
            write_strategy: Default::default(),
            parser,
            max_size: super::BLOCK_SIZE,
            timeout_write: Timeout::from_millis(200),
            timeout_read: Timeout::from_millis(100),
        }
    }
}

impl<E, P> discovery::Inited for MsgQue<E, P>
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

const PADDING: Hasher = Hasher::Padding(Padding);

impl<E, P> Hash for MsgQue<E, P>
where
    E: Endpoint,
    P: Protocol,
{
    #[inline]
    fn hash<K: HashKey>(&self, k: &K) -> i64 {
        PADDING.hash(k)
    }
}

impl<E, Req, P> Topology for MsgQue<E, P>
where
    E: Endpoint<Item = Req>,
    Req: Request,
    P: Protocol,
{
    #[inline]
    fn exp_sec(&self) -> u32 {
        log::error!("msg queue does't support expire");
        assert!(false, "msg queue does't support expire");
        0
    }
}

//TODO: 验证的时候需要考虑512字节这种边界msg
impl<E, Req, P> Endpoint for MsgQue<E, P>
where
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

        // 队列始终不需要write back，即写成功后不需要继续写
        req.write_back(false);

        // 对于读请求：顺序读取队列，如果队列都去了到数据，就连续读N个，如果没读到，则尝试下一个ip，直到轮询完所有的ip
        // 注意空读后的最后一次请求，会概率尝试访问offline
        if req.operation().is_retrival() {
            let (get_offline, qid) = self.get_read_idx(&ctx, inited, rw_count);
            assert!(
                (get_offline && qid < self.streams_offline.len())
                    || (!get_offline && qid < self.streams_read.len())
            );

            if !get_offline {
                ctx.update_qid(qid as u16);
            }
            // 是否重试：之前重试次数小于阀值-1，且不是从offline streams获取(offline是最后一次获取)
            req.try_next(rw_count < (READ_RETRY_COUNT - 1) && !get_offline);
            *req.mut_context() = ctx.ctx;
            log::debug!(
                "+++ mcq get {} from qid/{}, from_offline/{} req: {:?}",
                self.service,
                qid,
                get_offline,
                req.data()
            );
            if !get_offline {
                self.streams_read.get(qid).unwrap().1.send(req);
            } else {
                self.streams_offline.get(qid).unwrap().1.send(req);
            }
            return;
        }

        // 写请求，需要根据消息的长度进行分组轮询写入
        let mut wsize = ctx.get_write_size();
        if wsize == 0 {
            wsize = req.len();
        }
        let (qid, wsize) = self.write_strategy.next_queue_write(wsize);
        ctx.update_write_size(wsize);
        ctx.update_qid(qid);
        req.try_next(rw_count < WRITE_RETRY_COUNT);
        *req.mut_context() = ctx.ctx;

        log::debug!(
            "+++ will send mcq to {}/{}/{}, req:{:?}",
            wsize,
            qid,
            rw_count,
            req.data()
        );

        let streams = self.streams_write.get(&wsize).unwrap();
        let s = streams.get(qid as usize).unwrap();
        s.1.send(req)
    }
}

//获得待读取的streams和qid，返回的bool指示是否从offline streams中读取，true为都offline stream
impl<E, P> MsgQue<E, P>
where
    E: Endpoint,
    P: Protocol,
{
    fn get_read_idx(&self, ctx: &Context, inited: bool, rw_count: usize) -> (bool, usize) {
        // 本request前一次访问的idx
        let last_qid = if !inited {
            None
        } else {
            Some(ctx.get_last_qid())
        };

        if rw_count < (READ_RETRY_COUNT - 1) {
            let qid = self.read_strategy.next_queue_read(last_qid);
            return (false, qid as usize);
        } else {
            // 请求空后，最后一次访问，10%的概率尝试访问offline队列
            if self.streams_offline.len() > 0 {
                if self.offline_time.elapsed().as_secs() < OFFLINE_STOP_READ_SECONDS {
                    if rand::random::<u32>() % 10 == 0 {
                        let oqid = self.offline_hits.fetch_add(1, Ordering::Relaxed)
                            % self.streams_offline.len();
                        return (true, oqid);
                    }
                }
            }

            // offline stream 窗口期已过，仍然从在线队列读取
            let qid = self.read_strategy.next_queue_read(last_qid);
            (false, qid as usize)
        }
    }
}

impl<E, P> MsgQue<E, P>
where
    E: Endpoint,
    P: Protocol,
{
    // 构建下线的队列
    fn build_offline(&mut self, sized_queue: &BTreeMap<usize, Vec<String>>) -> Vec<(String, E)> {
        let mut new_addrs = HashSet::with_capacity(self.streams_read.len());
        let _ = sized_queue
            .iter()
            .map(|(_, ss)| new_addrs.extend(ss.clone()));
        for (name, _, _) in self.streams_read.iter() {
            if !new_addrs.contains(name) {
                self.streams_offline.push((
                    name.clone(),
                    E::build(
                        name,
                        self.parser.clone(),
                        Resource::MsgQue,
                        name,
                        self.timeout_read,
                    ),
                ));
            }
        }

        // TODO: 如果offline中有ip重新上线，清理掉
        let mut offline = Vec::with_capacity(self.streams_offline.len());
        let old = self.streams_offline.split_off(0);
        for (name, s) in old.into_iter() {
            if !new_addrs.contains(&name) {
                offline.push((name, s));
            }
        }

        offline
    }

    fn build_read_streams(
        &self,
        old: &mut HashMap<String, E>,
        addrs: &BTreeMap<usize, Vec<String>>,
        name: &str,
        timeout: Timeout,
    ) -> Vec<(String, E, usize)> {
        let mut streams = Vec::with_capacity(addrs.len());
        for (size, servs) in addrs.iter() {
            for s in servs.iter() {
                streams.push((s, size));
            }
        }
        streams
            .into_iter()
            .map(|(srv, size)| {
                (
                    srv.clone(),
                    old.remove(srv).unwrap_or(E::build(
                        srv,
                        self.parser.clone(),
                        Resource::MsgQue,
                        name,
                        timeout,
                    )),
                    *size,
                )
            })
            .collect()
    }

    fn build_write_stream(
        &mut self,
        addrs: &BTreeMap<usize, Vec<String>>,
        name: &str,
        timeout: Timeout,
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
                            old_streams.remove(addr).unwrap_or(E::build(
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

    // TODO: 下线ips在清理时间下线，如何触发？
    // fn clean_offline_streams(&mut self) {
    //     log::info!(
    //         "msgque {} will clean offline streams/{}",
    //         self.streams_offline.len()
    //     );
    //     if self.offline_time.elapsed().as_secs() > OFFLINE_CLEAN_SECONDS {
    //         self.streams_offline.clear();
    //         self.offline_hits.store(0, Ordering::Relaxed);
    //     }
    // }
}

impl<E, P> TopologyWrite for MsgQue<E, P>
where
    P: Protocol,
    E: Endpoint,
{
    #[inline]
    fn update(&mut self, name: &str, cfg: &str) {
        if let Some(ns) = super::config::Namespace::try_from(cfg, name) {
            log::debug!("+++ updating msgque for {}", name);
            assert!(ns.sized_queue.len() > 0, "msgque: {}, cfg:{}", name, cfg);

            self.service = name.to_string();

            self.timeout_read.adjust(ns.timeout_read);
            self.timeout_write.adjust(ns.timeout_write);

            let old_r = self.streams_read.split_off(0);
            let mut old_streams_read: HashMap<String, E> =
                old_r.into_iter().map(|(addr, e, _)| (addr, e)).collect();

            // 首先构建offline stream，如果前一次下线ips已经超时，先清理
            if self.offline_time.elapsed().as_secs() > OFFLINE_CLEAN_SECONDS
                && self.streams_offline.len() > 0
            {
                self.streams_offline.clear();
                log::info!(
                    "clear mcq last offline ips:{}/{}",
                    name,
                    self.streams_offline.len()
                );
            }
            self.streams_offline = self.build_offline(&ns.sized_queue);
            self.offline_time = Instant::now();
            self.offline_hits.store(0, Ordering::Relaxed);

            // 构建读stream
            self.streams_read = self.build_read_streams(
                &mut old_streams_read,
                &ns.sized_queue,
                name,
                self.timeout_read,
            );
            let readers_assit = self
                .streams_read
                .iter()
                .enumerate()
                .map(|(i, (_, _, size))| Node::from(i as QID, *size))
                .collect();
            self.read_strategy = HitFirstReader::from(readers_assit).into();

            // 构建写stream
            self.streams_write = self.build_write_stream(&ns.sized_queue, name, self.timeout_write);
            let writers_assist = self
                .streams_write
                .iter()
                .map(|(k, v)| {
                    (
                        *k,
                        v.iter().enumerate().map(|(i, (_, _))| i as QID).collect(),
                    )
                })
                .collect();
            self.write_strategy = RobbinWriter::from(writers_assist).into();
            log::debug!(
                "+++ updated msgque for offline/{}, reads/{}, writes/{}",
                self.streams_offline.len(),
                self.streams_read.len(),
                self.streams_write.len()
            );

            // TODO: 10分钟后，清理offline streams
            // rt::spawn(async move {
            //     tokio::time::sleep(Duration::from_secs(OFFLINE_CLEAN_SECONDS)).await;
            //     self.clean_offline_streams();
            // });
        }
    }
}
