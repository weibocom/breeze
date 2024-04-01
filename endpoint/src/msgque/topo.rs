use discovery::TopologyWrite;

use protocol::{Protocol, Request, Resource};
use rand::{rngs::ThreadRng, seq::SliceRandom, thread_rng, RngCore};

use core::fmt;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{cell::RefCell, time::Instant};

use super::config::Namespace;
use super::{
    strategy::{Fixed, RoundRobbin},
    ReadStrategy, WriteStrategy,
};
use crate::dns::{DnsConfig, DnsLookup};
use crate::{Endpoint, Timeout, Topology};
use sharding::hash::{Hash, HashKey, Hasher, Padding};
use std::sync::atomic::Ordering::Relaxed;

// 读miss后的重试次数
const READ_RETRY_COUNT: usize = 2;
// 写失败后的重试次数
const WRITE_RETRY_COUNT: usize = 3;

// const READ_OFFLINE_MQ: bool = true;

// ip vintage下线后，20分钟后停止读
const OFFLINE_LIMIT_SECONDS: u64 = 60 * 20;

/// Clone时，需要对读写队列乱序，所以单独实现
#[derive(Debug, Clone)]
pub struct MsgQue<E, P> {
    service: String,

    // TODO：目前只是每个业务topo随机，后续尝试做到每个topo的clone都随机 fishermen
    // 读队列，随机乱序
    readers: Vec<(E, String, usize)>,
    // 写队列，按size递增放置，相同size的队列随机放置
    writers: Vec<(E, String, usize)>,

    // streams_write: BTreeMap<usize, Vec<(String, E)>>,

    // 轮询访问，N分钟后下线
    readers_offline: Vec<(E, String, usize)>,

    reader_strategy: RoundRobbin,
    writer_strategy: Fixed,

    can_read_offline: Arc<AtomicBool>,
    offline_time: Instant,
    // read_strategy: Arc<HitFirstReader>,
    // write_strategy: Arc<RobbinWriter>,
    parser: P,
    // 占位hasher，暂时不需要真实计算
    // max_size: usize,
    timeout_write: Timeout,
    timeout_read: Timeout,

    cfg: Box<DnsConfig<Namespace>>,
}

thread_local! {
    static THREAD_RNG : RefCell<ThreadRng> = RefCell::new(thread_rng());
}

impl<E, P> From<P> for MsgQue<E, P> {
    #[inline]
    fn from(parser: P) -> Self {
        Self {
            service: Default::default(),
            readers: Default::default(),
            writers: Default::default(),
            readers_offline: Default::default(),
            can_read_offline: Arc::new(AtomicBool::new(false)),
            offline_time: Instant::now(),
            reader_strategy: Default::default(),
            writer_strategy: Default::default(),
            parser,
            timeout_write: Timeout::from_millis(200),
            timeout_read: Timeout::from_millis(100),
            cfg: Default::default(),
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
        let read_inited = self.readers.len() > 0
            && self
                .readers
                .iter()
                .fold(true, |inited, e| inited && e.0.inited());
        if !read_inited {
            return false;
        }

        // check write streams
        if self.writers.len() > 0 {
            for (stream, _name, _size) in self.writers.iter() {
                if !stream.inited() {
                    return false;
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
        let inited = ctx.inited();

        // 将访问次数加一，并返回之前的访问次数
        let tried_count = ctx.get_and_incr_tried_count();

        // 队列始终不需要write back，即写成功后不需要继回写
        req.write_back(false);

        // 对于读请求：顺序读取队列，如果队列都去了到数据，就连续读N个，如果没读到，则尝试下一个ip，直到轮询完所有的ip
        // 注意空读后的最后一次请求，会概率尝试访问offline
        if req.operation().is_retrival() {
            let (frm_offline, qid) = self.get_read_idx(inited, tried_count);

            if !frm_offline {
                ctx.update_qid(qid as u16);
            }
            // 是否重试，按设置的读重试次数
            req.try_next(tried_count < (READ_RETRY_COUNT - 1));
            *req.mut_context() = ctx.ctx;
            log::debug!(
                "+++ mq {} send get to: {}/{}, tried:{}, req:{:?}",
                self.service,
                qid,
                frm_offline,
                tried_count,
                req
            );
            if !frm_offline {
                assert!(qid < self.readers.len());
                self.readers.get(qid).expect("mq").0.send(req);
            } else {
                assert!(qid < self.readers_offline.len());
                self.readers_offline.get(qid).expect("mqo").0.send(req);
            }
            return;
        }

        let last_qid = ctx.get_last_qid(inited);
        let qid = self.writer_strategy.get_write_idx(req.len(), last_qid);
        ctx.update_qid(qid as u16);
        req.try_next(tried_count < WRITE_RETRY_COUNT);
        *req.mut_context() = ctx.ctx;
        log::debug!("+++ last qid:{:?}", last_qid);
        log::debug!(
            "+++ mq {} send set to: {}, tried:{}, req:{:?}",
            self.service,
            qid,
            tried_count,
            req
        );

        assert!((qid as usize) < self.writers.len(), "qid:{}", qid);
        let stream = self.writers.get(qid as usize).expect("mq");
        stream.0.send(req)
    }
}

//获得待读取的streams和qid，返回的bool指示是否从offline streams中读取，true为都offline stream
impl<E, P> MsgQue<E, P>
where
    E: Endpoint,
    P: Protocol,
{
    fn get_read_idx(&self, inited: bool, tried_count: usize) -> (bool, usize) {
        // ctx未初始化，说明是第一次访问，直接获取一个stream
        if !inited {
            let qid = self.reader_strategy.get_read_idx();
            return (false, qid as usize);
        }

        // 如果可以读下线queue，把最后一次访问的1%的概率留给访问下线队列，下线队列最多读20分钟
        if self.readers_offline.len() > 0
            && tried_count == (READ_RETRY_COUNT - 1)
            && self.can_read_offline.load(Relaxed)
        {
            // 请求空后，最后一次访问，1%的概率尝试访问offline队列
            let rand = THREAD_RNG.with(|rng| rng.borrow_mut().next_u32() % 100) as usize;
            if rand % 100 == 1 {
                if self.offline_time.elapsed().as_secs() > OFFLINE_LIMIT_SECONDS {
                    self.can_read_offline.store(false, Relaxed);
                }
                return (true, rand % self.readers_offline.len());
            }
        }

        // 不读下线队列，则按照既定策略获取下一个stream
        let qid = self.reader_strategy.get_read_idx();
        return (false, qid as usize);
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
    fn build_readers(&mut self, new_ques: &Vec<(String, usize)>) {
        let old_r = self.readers.split_off(0);
        let mut old_readers: HashMap<String, (E, usize)> = old_r
            .into_iter()
            .map(|(ept, addr, qsize)| (addr, (ept, qsize)))
            .collect();

        // 构建新的readers，并进行随机打乱顺序
        self.readers = new_ques
            .into_iter()
            .map(|(addr, qsize)| {
                (
                    old_readers
                        .remove(addr)
                        .map(|(ept, _)| ept)
                        .unwrap_or(E::build(
                            addr,
                            self.parser.clone(),
                            Resource::MsgQue,
                            &self.service,
                            self.timeout_read,
                        )),
                    addr.clone(),
                    *qsize,
                )
            })
            .collect();
        self.readers.shuffle(&mut thread_rng());

        // 将之前reader中剩余/下线的连接放置到offline中
        self.readers_offline.clear();
        for (addr, (ept, qsize)) in old_readers.into_iter() {
            self.readers_offline.push((ept, addr, qsize));
        }
        if self.readers_offline.len() > 0 {
            self.offline_time = Instant::now();
        }
    }

    fn build_writers(&mut self, new_ques: &Vec<(String, usize)>) {
        let old_w = self.writers.split_off(0);
        let mut old_writers: HashMap<String, E> = old_w
            .into_iter()
            .map(|(ept, addr, _)| (addr, ept))
            .collect();

        // 构建writer queues，同时记录每个size的起始位置
        self.writers = new_ques
            .iter()
            .map(|(addr, qsize)| {
                (
                    old_writers.remove(addr).unwrap_or(E::build(
                        &addr,
                        self.parser.clone(),
                        Resource::MsgQue,
                        &self.service,
                        self.timeout_write,
                    )),
                    addr.clone(),
                    *qsize,
                )
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

    #[inline]
    fn load_inner(&mut self) -> Option<()> {
        // 每个size的域名至少可以解析出一个ip，否则lookup应该失败
        let qaddrs = self.cfg.shards_url.lookup()?;
        let qsizes = &self.cfg.backends_qsize;
        assert_eq!(qaddrs.len(), qsizes.len(), "{:?}/{:?}", qaddrs, qsizes);

        // 将按size分的ip列表按顺序放置，记录每个size的que的起始位置
        let mut ordered_ques = Vec::with_capacity(qaddrs.len());
        let mut qsize_poses = Vec::with_capacity(qaddrs.len());
        for (i, adrs) in qaddrs.into_iter().enumerate() {
            let qs = qsizes[i];
            qsize_poses.push((qs, ordered_ques.len()));
            adrs.into_iter()
                .for_each(|addr| ordered_ques.push((addr, qs)));
        }

        // 构建读写队列
        self.build_readers(&ordered_ques);
        self.build_writers(&ordered_ques);

        // 设置读写策略
        self.reader_strategy = RoundRobbin::new(self.readers.len());
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
        }
    }

    #[inline]
    fn need_load(&self) -> bool {
        self.readers.len() < self.cfg.shards_url.len() || self.cfg.need_load()
    }

    #[inline]
    fn load(&mut self) -> bool {
        // TODO: 先改通知状态，再load，如果失败，改一个通用状态，确保下次重试，同时避免变更过程中新的并发变更，待讨论 fishermen
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
        let mut reader_str = String::with_capacity(256);
        let mut writers_str = String::with_capacity(256);
        self.readers
            .iter()
            .for_each(|r| reader_str.push_str(format!("{},", r.0.addr()).as_str()));
        self.writers
            .iter()
            .for_each(|w| writers_str.push_str(format!("{},", w.0.addr()).as_str()));

        write!(
            f,
            "mq - {} rstrategy:{}, wstrategy:{}, offline/{}, reads/{:?}, writes/{:?}",
            self.service,
            self.reader_strategy,
            self.writer_strategy,
            self.readers_offline.len(),
            reader_str,
            writers_str,
        )
    }
}
