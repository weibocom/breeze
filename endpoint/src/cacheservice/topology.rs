use protocol::{Protocol, Resource};
use rand::Rng;
use stream::{BackendBuilder, BackendStream};

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

#[derive(Clone)]
pub struct Topology<P> {
    pub(crate) hash: String,         // hash策略
    pub(crate) distribution: String, //distribution策略
    // 为了方便遍历
    l1_seq: Seq,
    // 处理写请求
    masters: Vec<String>,
    m_streams: HashMap<String, Arc<BackendBuilder>>,
    // 只用来同步写请求
    followers: Vec<Vec<String>>,
    f_streams: HashMap<String, Arc<BackendBuilder>>,
    // 处理读请求,每个layer选择一个，先打通
    // 包含多层，每层是一组资源池，比如在mc，一般有三层，分别为masterL1--master--slave--slaveL1: [layer[reader[node_dist_pool]]]
    layer_readers: Vec<Vec<Vec<String>>>,
    get_streams: HashMap<String, Arc<BackendBuilder>>,
    gets_streams: HashMap<String, Arc<BackendBuilder>>,

    parser: P,
}

impl<P> Topology<P> {
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

    pub fn retrive_get(&self) -> Vec<Vec<BackendStream>> {
        self.reader_layers(&self.get_streams, "get")
    }
    pub fn retrive_gets(&self) -> Vec<Vec<BackendStream>> {
        self.reader_layers(&self.gets_streams, "multi-get")
    }

    // 由于mc会将master也作为一个L1来访问，所以此处统一做排重处理
    fn random_reads(&self) -> Vec<Vec<String>> {
        let mut readers = Vec::new();
        for layer in &self.layer_readers {
            if layer.len() == 0 {
                log::warn!("empty layer in {:?}", self.layer_readers);
                continue;
            }
            // 纯粹的按seq，可能出现某种场景，导致每次循环选择的group都是一样。
            // 引入一个随机数，打破这种波动
            let fluctuation: bool = rand::thread_rng().gen();
            let delta = 1 + fluctuation as usize;
            let seq = self.l1_seq.fetch_add(delta, Ordering::Acquire);
            let idx = seq % layer.len();
            let r = &layer[idx];
            if !readers.contains(r) {
                readers.push(r.clone())
            }
        }

        readers
    }

    // 获取reader列表
    fn reader_layers(
        &self,
        streams: &HashMap<String, Arc<BackendBuilder>>,
        op: &str,
    ) -> Vec<Vec<BackendStream>> {
        // 从每个层选择一个reader
        let readers = self.random_reads();
        log::debug!("random {}-layers inited:readers: {:?}", op, readers);
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
    }

    fn update(&mut self, name: &str, cfg: &str)
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
                log::info!("parse config. error:{} name:{} cfg:{}", e, name, cfg.len());
                return;
            }
        };
        if self.masters.len() == 0 || self.layer_readers.len() == 0 {
            log::info!("cacheservice empty. {} => {}", name, cfg);
            return;
        }

        let c = stream::MAX_CONNECTIONS;
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
    }
}

impl<P> discovery::TopologyWrite for Topology<P>
where
    P: Send + Sync + Protocol,
{
    fn update(&mut self, name: &str, cfg: &str) {
        self.update(name, cfg);
    }
}

impl<P> From<P> for Topology<P> {
    fn from(parser: P) -> Self {
        Self {
            parser: parser,
            l1_seq: Seq::random(),
            hash: Default::default(),
            distribution: Default::default(),
            masters: Default::default(),
            m_streams: Default::default(),
            followers: Default::default(),
            f_streams: Default::default(),
            layer_readers: Default::default(),
            get_streams: Default::default(),
            gets_streams: Default::default(),
        }
    }
}

// 所有的stream都初始化完成
impl<P> discovery::Inited for Topology<P> {
    fn inited(&self) -> bool {
        for streams in vec![
            &self.m_streams,
            &self.f_streams,
            &self.get_streams,
            &self.gets_streams,
            //&self.meta_stream,
        ] {
            for (_, builder) in streams {
                if !builder.inited() {
                    return false;
                }
            }
        }

        true
    }
}

use std::fmt::{self, Display, Formatter};
impl<P> Display for Topology<P> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "master:{:?} followers:{:?} readers:{:?}",
            self.masters, self.followers, self.layer_readers
        )
    }
}

struct Seq {
    inner: AtomicUsize,
}
impl std::ops::Deref for Seq {
    type Target = AtomicUsize;
    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl Clone for Seq {
    fn clone(&self) -> Self {
        Self {
            inner: AtomicUsize::new(self.load(Ordering::Acquire)),
        }
    }
}
impl Seq {
    fn random() -> Self {
        // 一般情况下, 一层的sharding数量不会超过64k。
        let rd = rand::thread_rng().gen_range(0..65536);
        Self {
            inner: AtomicUsize::new(rd),
        }
    }
}
