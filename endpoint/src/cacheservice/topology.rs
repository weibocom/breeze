use rand::Rng;
use stream::{BackendBuilder, BackendStream};

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use protocol::{Protocol, Resource};

unsafe impl<P> Send for Topology<P> {}
unsafe impl<P> Sync for Topology<P> {}

#[derive(Default)]
pub struct Topology<P> {
    pub(crate) hash: String,         // hash策略
    pub(crate) distribution: String, //distribution策略
    // 最后一个元素是slave，倒数第二个元素是master，剩下的是l1.
    // 为了方便遍历
    l1_seq: AtomicUsize,
    // 处理写请求
    pub(crate) masters: Vec<String>,
    m_streams: HashMap<String, Arc<BackendBuilder>>,
    // 只用来同步写请求
    followers: Vec<Vec<String>>,
    f_streams: HashMap<String, Arc<BackendBuilder>>,
    // 处理读请求,每个layer选择一个，先打通
    // 包含多层，每层是一组资源池，比如在mc，一般有三层，分别为masterL1--master--slave--slaveL1: [layer[reader[node_dist_pool]]]
    pub(crate) layer_readers: Vec<Vec<Vec<String>>>,
    get_streams: HashMap<String, Arc<BackendBuilder>>,
    gets_streams: HashMap<String, Arc<BackendBuilder>>,

    metas: Vec<String>,
    meta_stream: HashMap<String, Arc<BackendBuilder>>,

    parser: P,
}

// 用来测试的一组配置, ip都是127.0.0.1
// master port: 11211:11212
// followers: 11213, 11214; 11215, 11216;
// l1: 11213, 11214; 11215, 11216
// 没有slave
impl<P> Topology<P> {
    pub fn meta(&self) -> Vec<BackendStream> {
        self.metas
            .iter()
            .map(|addr| {
                self.meta_stream
                    .get(addr)
                    .expect("stream must be exists before address")
                    .build()
            })
            .collect()
    }
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
    pub fn followers(&self) -> Vec<Vec<BackendStream>> {
        if self.followers.len() == 0 {
            return vec![];
        }

        self.followers
            .iter()
            .map(|servers| {
                servers
                    .iter()
                    .map(|addr| {
                        self.f_streams
                            .get(addr)
                            .expect("stream must be exists before address when call followers")
                            .build()
                    })
                    .collect()
            })
            .collect()
    }

    // 测试完毕后清理 fishermen 2021.7.2
    // TODO：这里只返回一个pool，后面会替换掉 fishermen
    // pub fn next_l1(&self) -> Vec<BackendStream> {
    //     if self.layer_readers.len() == 0 {
    //         return vec![];
    //     }
    //     let idx = self.l1_seq.fetch_add(1, Ordering::AcqRel) % self.readers.len();
    //     unsafe {
    //         self.random_reads()
    //             .get_unchecked(idx)
    //             .iter()
    //             .map(|addr| {
    //                 self.get_streams
    //                     .get(addr)
    //                     .expect("stream must be exists before address")
    //                     .build()
    //             })
    //             .collect()
    //     }
    // }
    // TODO：这里只返回一个pool，后面会替换掉 fishermen
    // pub fn next_l1_gets(&self) -> Vec<BackendStream> {
    //     if self.readers.len() == 0 {
    //         return vec![];
    //     }
    //     let idx = self.l1_seq.fetch_add(1, Ordering::AcqRel) % self.readers.len();
    //     unsafe {
    //         self.random_reads()
    //             .get_unchecked(idx)
    //             .iter()
    //             .map(|addr| {
    //                 self.gets_streams
    //                     .get(addr)
    //                     .expect("stream must be exists before address")
    //                     .build()
    //             })
    //             .collect()
    //     }
    // }

    pub fn retrive_get(&self) -> Vec<Vec<BackendStream>> {
        self.reader_layers(&self.get_streams)
    }
    pub fn retrive_gets(&self) -> Vec<Vec<BackendStream>> {
        self.reader_layers(&self.gets_streams)
    }

    // 由于mc会将master也作为一个L1来访问，所以此处统一做排重处理
    fn random_reads(&self) -> Vec<Vec<String>> {
        let mut readers = Vec::new();
        for layer in &self.layer_readers {
            if layer.len() == 1 {
                let r = &layer[0];
                if !readers.contains(r) {
                    readers.push(r.clone());
                }
            } else if layer.len() > 1 {
                let rd = rand::thread_rng().gen_range(0..layer.len());
                let r = &layer[rd];
                if !readers.contains(r) {
                    readers.push(r.clone())
                }
            } else {
                log::warn!("topolody - rand readers should has candidates!");
            }
        }

        log::info!("use random readers: {:?}", readers);
        readers
    }

    // 获取reader列表
    fn reader_layers(
        &self,
        streams: &HashMap<String, Arc<BackendBuilder>>,
    ) -> Vec<Vec<BackendStream>> {
        // 从每个层选择一个reader
        let readers = self.random_reads();
        readers
            .iter()
            .map(|pool| {
                pool.iter()
                    .map(|addr| {
                        streams
                            .get(addr)
                            .expect("stream must be exists before adress")
                            .build()
                    })
                    .collect()
            })
            .collect()
    }

    // 删除不存在的stream
    fn delete_non_exists(addrs: &[String], streams: &mut HashMap<String, Arc<BackendBuilder>>) {
        streams.retain(|addr, _| addrs.contains(addr));
    }
    // 添加新增的stream
    fn add_new(
        parser: &P,
        addrs: &[String],
        streams: &mut HashMap<String, Arc<BackendBuilder>>,
        parallel: usize,
        namespace: &str,
    ) where
        P: Send + Sync + Protocol + 'static + Clone,
    {
        for addr in addrs {
            if !streams.contains_key(addr) {
                streams.insert(
                    addr.to_string(),
                    Arc::new(BackendBuilder::from(
                        parser.clone(),
                        addr,
                        parallel,
                        Resource::Memcache,
                        namespace,
                    )),
                );
            }
        }
    }

    fn update_from_namespace(&mut self, ns: super::Namespace) {
        let (masters, followers, readers, hash, distribution) = ns.into_split();
        self.masters = masters;
        self.followers = followers;
        self.layer_readers = readers;
        self.hash = hash;
        self.distribution = distribution;
        //self.metas = self.readers.clone().into_iter().flatten().collect();
        self.metas = self.masters.clone();

        self.correct_hash_distribution();
    }

    // 根据java client转换逻辑，对hash、distribution进行转换
    fn correct_hash_distribution(&mut self) {
        // java 只支持下面三种组合，其他的统统不支持，rust对其他类型，强制改为java的默认方式？ fishermen
        if (self.hash.eq(hash::HASH_BKDR) && self.distribution.eq(hash::DISTRIBUTION_CONSISTENT))
            || (self.hash.eq(hash::HASH_BKDR) && self.distribution.eq(hash::DISTRIBUTION_MODULA))
            || (self.hash.eq(hash::HASH_CRC32) && self.distribution.eq(hash::DISTRIBUTION_MODULA))
        {
            return;
        }

        // 对于其他组合模式，强制改为默认行为
        log::warn!(
            "!!! found malformed hash/distribution: {}/{}, will change to crc32/mod",
            self.hash,
            self.distribution
        );
        self.hash = hash::HASH_CRC32.to_string();
        self.distribution = hash::DISTRIBUTION_MODULA.to_string();
    }

    fn update(&mut self, cfg: &str, name: &str)
    where
        P: Send + Sync + Protocol + 'static + Clone,
    {
        let p = self.parser.clone();
        let idx = name.find(':').unwrap_or(name.len());
        if idx == 0 || idx >= name.len() - 1 {
            log::info!("not a valid cache service name:{} no namespace found", name);
            return;
        }
        let namespace = &name[idx + 1..];

        match super::Namespace::parse(cfg, namespace) {
            Ok(ns) => self.update_from_namespace(ns),
            Err(e) => {
                log::info!("parse cacheservice config error: name:{} error:{}", name, e);
                return;
            }
        };
        if self.masters.len() == 0 || self.layer_readers.len() == 0 {
            log::info!("cacheservice empty. {} => {}", name, cfg);
            return;
        }

        let c = 256;
        Self::delete_non_exists(&self.masters, &mut self.m_streams);
        Self::add_new(&p, &self.masters, &mut self.m_streams, c, namespace);

        let followers: Vec<String> = self.followers.clone().into_iter().flatten().collect();
        Self::delete_non_exists(&followers, &mut self.f_streams);
        Self::add_new(&p, followers.as_ref(), &mut self.f_streams, c, namespace);

        let readers: Vec<String> = self
            .layer_readers
            .clone()
            .into_iter()
            .flatten()
            .flatten()
            .collect();
        // get command
        Self::delete_non_exists(&readers, &mut self.get_streams);
        Self::add_new(&p, &readers, &mut self.get_streams, c, namespace);
        // get[s] command
        Self::delete_non_exists(&readers, &mut self.gets_streams);
        Self::add_new(&p, &readers, &mut self.gets_streams, c, namespace);

        // meta
        Self::delete_non_exists(&self.metas, &mut self.meta_stream);
        Self::add_new(&p, &self.metas, &mut self.meta_stream, c, namespace);
    }
}

impl<P> Clone for Topology<P>
where
    P: Clone,
{
    fn clone(&self) -> Self {
        Self {
            hash: self.hash.clone(),
            distribution: self.distribution.clone(),
            l1_seq: AtomicUsize::new(self.l1_seq.load(Ordering::Acquire)),
            masters: self.masters.clone(),
            m_streams: self.m_streams.clone(),
            followers: self.followers.clone(),
            f_streams: self.f_streams.clone(),
            layer_readers: self.layer_readers.clone(),
            get_streams: self.get_streams.clone(),
            gets_streams: self.gets_streams.clone(),
            metas: self.metas.clone(),
            meta_stream: self.meta_stream.clone(),
            parser: self.parser.clone(),
        }
    }
}

impl<P> discovery::Topology for Topology<P>
where
    P: Send + Sync + Protocol,
{
    fn update(&mut self, cfg: &str, name: &str) {
        log::info!("cache service topology received:{}", name);
        self.update(cfg, name);
        log::info!("master:{:?}", self.masters);
    }
}
impl<P> left_right::Absorb<(String, String)> for Topology<P>
where
    P: Send + Sync + Protocol + 'static + Clone,
{
    fn absorb_first(&mut self, cfg: &mut (String, String), _other: &Self) {
        self.update(&cfg.0, &cfg.1);
    }
    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }
}

impl<P> From<P> for Topology<P> {
    fn from(parser: P) -> Self {
        Self {
            parser: parser,
            l1_seq: AtomicUsize::new(0),
            hash: Default::default(),
            distribution: Default::default(),
            masters: Default::default(),
            m_streams: Default::default(),
            followers: Default::default(),
            f_streams: Default::default(),
            layer_readers: Default::default(),
            get_streams: Default::default(),
            gets_streams: Default::default(),
            metas: Default::default(),
            meta_stream: Default::default(),
        }
    }
}
