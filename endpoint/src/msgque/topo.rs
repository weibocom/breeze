use discovery::TopologyWrite;

use protocol::{Protocol, Request, Resource};
use rand::{rngs::ThreadRng, seq::SliceRandom, thread_rng};

use core::fmt;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::{cell::RefCell, time::Instant};

use super::config::Namespace;
use super::{
    strategy::{Fixed, RoundRobbin},
    ReadStrategy, WriteStrategy,
};
use crate::dns::{DnsConfig, DnsLookup};
use crate::{Endpoint, Timeout, Topology};
use sharding::hash::{Hash, HashKey, Hasher, Padding};

// TODO 目前重试暂时都设置为1，因为总的retries目前是1，需要讨论是否有必须要调大 fishermen
// 读mq读空后，最大的尝试次数
const MAX_RETRIES_READ: usize = 1;
// 写mq失败后，最大尝试次数
const MAX_RETRIES_WRITE: usize = 1;

// const READ_OFFLINE_MQ: bool = true;

// ip vintage下线后，2分钟后真正从读列表中下线，写列表是立即下线的
const OFFLINE_LIMIT_SECONDS: u64 = 60 * 2;

/// Clone时，需要对读写队列乱序，所以单独实现
#[derive(Debug, Clone)]
pub struct MsgQue<E, P> {
    service: String,

    // TODO: 目前只是每个实例的topo初始化时随机一次，故mesh实例内写入位置固定，后续考虑改为每个连接都随机？

    //所有的后端连接，包括已经下线的
    backends: Vec<(E, usize)>,

    // 写队列，按size递增放置，相同size的队列随机放置，只包括线上的队列
    writers: Vec<usize>,

    // streams_write: BTreeMap<usize, Vec<(String, E)>>,

    // TODO：轮询访问，2分钟后下线，期间一直need_load
    // readers_offline: Vec<(E, usize)>,
    reader_strategy: RoundRobbin,
    writer_strategy: Fixed,

    // can_read_offline: Arc<AtomicBool>,
    // read_strategy: Arc<HitFirstReader>,
    // write_strategy: Arc<RobbinWriter>,
    parser: P,
    // 占位hasher，暂时不需要真实计算
    // max_size: usize,
    timeout_write: Timeout,
    timeout_read: Timeout,

    cfg: Box<DnsConfig<Namespace>>,
    cfg_updated_time: Instant,
}

thread_local! {
    static THREAD_RNG : RefCell<ThreadRng> = RefCell::new(thread_rng());
}

impl<E, P> From<P> for MsgQue<E, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            service: Default::default(),
            backends: Default::default(),
            // readers: Default::default(),
            writers: Default::default(),
            // readers_offline: Default::default(),
            // can_read_offline: Arc::new(AtomicBool::new(false)),
            // offline_time: Instant::now(),
            reader_strategy: Default::default(),
            writer_strategy: Default::default(),
            parser,
            timeout_write: Timeout::from_millis(200),
            timeout_read: Timeout::from_millis(100),
            cfg: Default::default(),
            cfg_updated_time: Instant::now(),
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
        self.backends.len() > 0
            && self
                .backends
                .iter()
                .fold(true, |inited, e| inited && e.0.inited())
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
        let inited = ctx.inited();

        // 将访问次数加一，并返回之前的访问次数
        let tried_count = ctx.get_and_incr_tried_count();

        // 队列始终不需要write back，即写成功后不需要继回写
        req.write_back(false);

        // 对于读请求：顺序读取队列，如果队列都去了到数据，就连续读N个，如果没读到，则尝试下一个ip，直到轮询完所有的ip
        // 注意空读后的最后一次请求，会概率尝试访问offline
        let (qid, try_next) = if req.operation().is_retrival() {
            let qid = self.reader_strategy.get_read_idx();
            let try_next = tried_count < MAX_RETRIES_READ;
            (qid, try_next)
        } else {
            debug_assert!(req.operation().is_store());

            let last_wid = ctx.get_last_qid(inited);
            let wid = self.writer_strategy.get_write_idx(req.len(), last_wid);
            ctx.update_qid(wid as u16);
            let try_next = tried_count < MAX_RETRIES_WRITE;

            assert!(wid < self.writers.len(), "{}/{}", wid, self);
            (*self.writers.get(wid).expect("mq write"), try_next)
        };

        req.try_next(try_next);
        req.retry_on_rsp_notok(true);
        *req.mut_context() = ctx.ctx;

        log::debug!(
            "+++ mq {} send to: {}, tried:{}, req:{:?}",
            self.service,
            qid,
            tried_count,
            req
        );

        assert!((qid as usize) < self.backends.len(), "qid:{}/{}", qid, self);
        let stream = self.backends.get(qid as usize).expect("mq");
        stream.0.send(req)
    }
}

impl<E, P> MsgQue<E, P>
where
    E: Endpoint,
    P: Protocol,
{
    // // 将原来的读队列中下线的ip，放到offline队列中
    // fn build_offline(&mut self, sized_queue: &Vec<(String, usize)>) -> Vec<(String, E)> {
    //     let mut new_addrs: HashSet<&String> = sized_queue
    //         .iter()
    //         .map(|(addr, _)| addr).collect();
    //     for (ept, name, _) in self.readers.iter() {
    //         if !new_addrs.contains(name) {
    //             self.streams_offline.push((
    //                 name.clone(),
    //                 ept,
    //             ));
    //         }
    //     }

    //     // TODO: 如果offline中有ip重新上线，清理掉
    //     let mut offline = Vec::with_capacity(self.streams_offline.len());
    //     let old = self.streams_offline.split_off(0);
    //     for (name, s) in old.into_iter() {
    //         if !new_addrs.contains(&name) {
    //             offline.push((name, s));
    //         }
    //     }

    //     offline
    // }

    /// 同时构建读队列 和 offline读队列
    fn build_backends(&mut self, new_ques: &Vec<(String, usize)>) {
        let mut old_backends: HashMap<String, (E, usize)> = self
            .backends
            .split_off(0)
            .into_iter()
            .map(|(ept, qsize)| (ept.addr().to_string(), (ept, qsize)))
            .collect();

        // 构建新的readers，并进行随机打乱顺序
        self.backends = new_ques
            .into_iter()
            .map(|(addr, qsize)| {
                (
                    old_backends
                        .remove(addr)
                        .map(|(ept, _)| ept)
                        .unwrap_or(E::build(
                            addr,
                            self.parser.clone(),
                            Resource::MsgQue,
                            &self.service,
                            self.timeout_read,
                        )),
                    *qsize,
                )
            })
            .collect();

        // 将线上ip列表乱序，以方便负载均衡
        self.backends.shuffle(&mut thread_rng());

        // 配置变更的2分钟内，将下线ip保留在backends中，正常读，但不写;2分钟后，不再保留下线ip了。
        if old_backends.len() > 0
            && self.cfg_updated_time.elapsed().as_secs() <= OFFLINE_LIMIT_SECONDS
        {
            old_backends
                .into_iter()
                .for_each(|(_k, (ep, qsize))| self.backends.push((ep, qsize)));
        }
    }

    fn build_writers(&mut self, new_ques: &Vec<(String, usize)>) {
        self.writers.clear();
        let idxes: HashMap<String, usize> = self
            .backends
            .iter()
            .enumerate()
            .map(|(idx, ep)| (ep.0.addr().to_string(), idx))
            .collect();

        // 构建writer queues，同时记录每个size的起始位置
        self.writers = new_ques
            .iter()
            .map(|(addr, qsize)| {
                // 考虑同一个IP端口复用多处的场景，此处不可用remove fishermen
                assert!(idxes.contains_key(addr), ":addr/{}in {}", qsize, self);
                idxes.get(addr).expect("mq").clone()
            })
            .collect();
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

    /// 2分钟之内，保持下线的ip在读列表中；2分钟后，将会使下线的ip真正清理掉
    #[inline]
    fn load_inner(&mut self) -> Option<()> {
        // 每个size的域名至少可以解析出一个ip，否则lookup应该失败
        let qaddrs = self.cfg.shards_url.lookup()?;
        let qsizes = &self.cfg.backends_qsize;
        assert_eq!(qaddrs.len(), qsizes.len(), "{:?}/{:?}", qaddrs, qsizes);

        // 将按size分的ip列表按顺序放置，记录每个size的que的起始位置
        let mut ordered_ques = Vec::with_capacity(qaddrs.len());
        let mut qsize_poses = Vec::with_capacity(qaddrs.len());
        let mut rng = thread_rng();
        for (i, mut adrs) in qaddrs.into_iter().enumerate() {
            let qs = qsizes[i];
            qsize_poses.push((qs, ordered_ques.len()));

            // 对每个size的ip列表进行随机排序
            adrs.shuffle(&mut rng);
            adrs.into_iter()
                .for_each(|addr| ordered_ques.push((addr, qs)));
        }

        // 构建读写队列，backends用于读，writers用于写
        self.build_backends(&ordered_ques);
        self.build_writers(&ordered_ques);

        // 设置读写策略
        self.reader_strategy = RoundRobbin::new(self.backends.len());
        self.writer_strategy = Fixed::new(self.writers.len(), &qsize_poses);

        log::debug!("+++ mq loaded: {}", self);

        Some(())
    }
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

            // 设置topo元数据
            self.service = name.to_string();
            self.timeout_read.adjust(ns.basic.timeout_read);
            self.timeout_write.adjust(ns.basic.timeout_write);

            self.cfg.update(name, ns);

            // 配置变更后，设置更新时间
            self.cfg_updated_time = Instant::now();
        }
    }

    // backends、writers长度不一致的时候，且大于2分钟，都需要load
    #[inline]
    fn need_load(&self) -> bool {
        if self.cfg.need_load() {
            return true;
        }
        // 如果backends、writers长度不一致，说明有下线ip，2分钟后，则需要load，将下线的ip从backends清理
        assert!(self.backends.len() >= self.writers.len(), "{}", self);
        if self.backends.len() > self.writers.len() {
            if self.cfg_updated_time.elapsed().as_secs() > OFFLINE_LIMIT_SECONDS {
                return true;
            }
        }
        false
    }

    #[inline]
    fn load(&mut self) -> bool {
        self.cfg
            .load_guard()
            .check_load(|| self.load_inner().is_some())
    }
}

impl<E, P> Display for MsgQue<E, P>
where
    P: Protocol,
    E: Endpoint,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut backends = String::with_capacity(256);
        self.backends
            .iter()
            .for_each(|(ep, s)| backends.push_str(format!("{}/{},", ep.addr(), s).as_str()));

        let sec = self.cfg_updated_time.elapsed().as_secs();
        write!(
            f,
            "mq - {} rstrategy:{}, wstrategy:{}, backends/{:?}, writes/{:?}, changed: {}",
            self.service, self.reader_strategy, self.writer_strategy, backends, self.writers, sec
        )
    }
}
