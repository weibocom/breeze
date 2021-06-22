use rand::Rng;
use stream::{BackendBuilder, Cid, RingBufferStream};

use protocol::memcache::MemcacheResponseParser;

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::net::tcp::OwnedWriteHalf;

use crate::cacheservice::memcache::memcache_topo::MemcacheConf;

type BackendStream = stream::BackendStream<Arc<RingBufferStream>, Cid>;

unsafe impl Send for Topology {}
unsafe impl Sync for Topology {}

#[derive(Default)]
pub struct Topology {
    // 最后一个元素是slave，倒数第二个元素是master，剩下的是l1.
    // 为了方便遍历
    l1_seq: AtomicUsize,
    // 处理写请求
    masters: Vec<String>,
    m_streams: HashMap<String, Arc<BackendBuilder>>,
    // 只用来同步写请求
    followers: Vec<Vec<String>>,
    f_streams: HashMap<String, Arc<BackendBuilder>>,
    // 处理读请求,每个layer选择一个，先打通
    // 后续考虑要调整为新的Vec嵌套逻辑： [random[reader[node_dist_pool]]]
    readers: Vec<Vec<String>>,
    get_streams: HashMap<String, Arc<BackendBuilder>>,
    gets_streams: HashMap<String, Arc<BackendBuilder>>,
}

// 用来测试的一组配置, ip都是127.0.0.1
// master port: 11211:11212
// followers: 11213, 11214; 11215, 11216;
// l1: 11213, 11214; 11215, 11216
// 没有slave
impl Topology {
    pub fn master(&self) -> Vec<BackendStream> {
        self.masters
            .iter()
            .map(|addr| {
                self.m_streams
                    .get(addr)
                    .expect("stream must be exists before address")
                    .build()
            })
            .collect()
    }
    // followers是只能写，读忽略的
    pub fn followers(&self) -> Vec<Vec<OwnedWriteHalf>> {
        vec![]
    }

    // TODO：这里只返回一个pool，后面会替换掉 fishermen
    pub fn next_l1(&self) -> Vec<BackendStream> {
        let idx = self.l1_seq.fetch_add(1, Ordering::AcqRel) % self.readers.len();
        unsafe {
            self.readers
                .get_unchecked(idx)
                .iter()
                .map(|addr| {
                    self.get_streams
                        .get(addr)
                        .expect("stream must be exists before address")
                        .build()
                })
                .collect()
        }
    }
    // TODO：这里只返回一个pool，后面会替换掉 fishermen
    pub fn next_l1_gets(&self) -> Vec<BackendStream> {
        let idx = self.l1_seq.fetch_add(1, Ordering::AcqRel) % self.readers.len();
        unsafe {
            self.readers
                .get_unchecked(idx)
                .iter()
                .map(|addr| {
                    self.gets_streams
                        .get(addr)
                        .expect("stream must be exists before address")
                        .build()
                })
                .collect()
        }
    }

    // 获取reader列表
    pub fn reader_4_get_through(&self) -> Vec<Vec<BackendStream>> {
        self.readers
            .iter()
            .map(|pool| {
                pool.iter()
                    .map(|addr| {
                        self.get_streams
                            .get(addr)
                            .expect("stream must be exists before adress")
                            .build()
                    })
                    .collect()
            })
            .collect()
    }

    fn insert(
        m: &mut HashMap<String, Arc<BackendBuilder>>,
        from: &HashMap<String, Arc<BackendBuilder>>,
        addr: &str,
        req: usize,
        resp: usize,
        parallel: usize,
        ignore: bool,
    ) {
        let stream = if let Some(old) = from.get(addr) {
            old.clone()
        } else {
            BackendBuilder::from_with_response::<MemcacheResponseParser>(
                addr.to_string(),
                req,
                resp,
                parallel,
                ignore,
            )
        };
        m.insert(addr.to_string(), stream);
    }
    fn parse(_cfg: &str) -> (Vec<String>, Vec<Vec<String>>, Vec<Vec<String>>) {
        let conf = MemcacheConf::parse_conf(_cfg);

        // master 就是conf中的master
        let master: Vec<String> = conf.master.clone();

        // followers包含： master-l1, slave, slave-l1
        let mut followers: Vec<Vec<String>> = conf.slave_l1.clone();
        followers.insert(0, conf.slave.clone());
        for l1 in conf.master_l1.clone() {
            followers.insert(0, l1);
        }

        // TODO：每个layer先选一个，后续再考虑l1负载均衡的问题
        // reader包含多种可能的读穿透顺序，每个读穿透都会包括：l1, master，slave
        let mut readers = Vec::new();
        if conf.master_l1.len() > 0 {
            let rd = rand::thread_rng().gen_range(0..conf.master_l1.len());
            let l1 = conf.master_l1.get(rd).unwrap().clone();
            readers.push(l1);
        }

        //let mut readers1 = conf.master_l1.clone();
        // l1随机选择一个，后续考虑在请求时，按请求随机，这样负载更均衡
        readers.push(conf.master.clone());
        readers.push(conf.slave.clone());

        // let masters = vec!["127.0.0.1:11211".to_string()];
        // let followers = vec![vec![]];
        // let readers = vec![vec!["127.0.0.1:11211".to_string()]];

        (master, followers, readers)
    }
    fn _copy(&self, cfg: &str) -> Self {
        let (masters, followers, readers) = Self::parse(cfg);

        let mut top: Topology = Self::default();

        let kb = 1024;
        let mb = 1024 * 1024;
        let p = 16;
        for addr in masters.iter() {
            Self::insert(&mut top.m_streams, &self.m_streams, addr, mb, kb, p, false)
        }

        for addr in followers.iter().flatten() {
            Self::insert(&mut top.f_streams, &self.f_streams, addr, mb, kb, p, true);
        }
        for addr in readers.iter().flatten() {
            Self::insert(
                &mut top.get_streams,
                &self.get_streams,
                addr,
                kb,
                mb,
                p,
                false,
            );
            Self::insert(
                &mut top.gets_streams,
                &self.gets_streams,
                addr,
                kb,
                mb,
                p,
                false,
            );
        }
        top.masters = masters;
        top.followers = followers;
        top.readers = readers;

        let l1_idx = rand::thread_rng().gen_range(0..top.readers.len());
        top.l1_seq = AtomicUsize::new(l1_idx);
        top
    }
}

impl Clone for Topology {
    fn clone(&self) -> Self {
        Self {
            l1_seq: AtomicUsize::new(self.l1_seq.load(Ordering::Acquire)),
            masters: self.masters.clone(),
            m_streams: self.m_streams.clone(),
            followers: self.followers.clone(),
            f_streams: self.f_streams.clone(),
            readers: self.readers.clone(),
            get_streams: self.get_streams.clone(),
            gets_streams: self.gets_streams.clone(),
        }
    }
}

impl discovery::Topology for Topology {
    fn copy_from(&self, cfg: &str) -> Self {
        self._copy(cfg)
    }
}
impl left_right::Absorb<String> for Topology {
    fn absorb_first(&mut self, cfg: &mut String, _other: &Self) {
        *self = discovery::Topology::copy_from(self, cfg);
    }
    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }
}
