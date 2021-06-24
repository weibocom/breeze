use stream::{BackendBuilder, Cid, RingBufferStream};

use protocol::memcache::MemcacheResponseParser;

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use tokio::net::tcp::OwnedWriteHalf;

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
    m_streams: HashMap<String, Arc<Mutex<BackendBuilder>>>,
    // 只用来同步写请求
    followers: Vec<Vec<String>>,
    f_streams: HashMap<String, Arc<Mutex<BackendBuilder>>>,
    // 处理读请求,每个layer选择一个，先打通
    // 后续考虑要调整为新的Vec嵌套逻辑： [random[reader[node_dist_pool]]]
    readers: Vec<Vec<String>>,
    get_streams: HashMap<String, Arc<Mutex<BackendBuilder>>>,
    gets_streams: HashMap<String, Arc<Mutex<BackendBuilder>>>,
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
                    .lock()
                    .unwrap()
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
        if self.readers.len() == 0 {
            return vec![];
        }
        let idx = self.l1_seq.fetch_add(1, Ordering::AcqRel) % self.readers.len();
        unsafe {
            self.readers
                .get_unchecked(idx)
                .iter()
                .map(|addr| {
                    self.get_streams
                        .get(addr)
                        .expect("stream must be exists before address")
                        .lock()
                        .unwrap()
                        .build()
                })
                .collect()
        }
    }
    // TODO：这里只返回一个pool，后面会替换掉 fishermen
    pub fn next_l1_gets(&self) -> Vec<BackendStream> {
        if self.readers.len() == 0 {
            return vec![];
        }
        let idx = self.l1_seq.fetch_add(1, Ordering::AcqRel) % self.readers.len();
        unsafe {
            self.readers
                .get_unchecked(idx)
                .iter()
                .map(|addr| {
                    self.gets_streams
                        .get(addr)
                        .expect("stream must be exists before address")
                        .lock()
                        .unwrap()
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
                            .lock()
                            .unwrap()
                            .build()
                    })
                    .collect()
            })
            .collect()
    }

    // 删除不存在的stream
    fn delete_non_exists(addrs: &[String], streams: &mut HashMap<String, Arc<Mutex<BackendBuilder>>>) {
        streams.retain(|addr, _| addrs.contains(addr));
    }
    // 添加新增的stream
    fn add_new(
        addrs: &[String],
        streams: &mut HashMap<String, Arc<Mutex<BackendBuilder>>>,
        req: usize,
        resp: usize,
        parallel: usize,
        ignore: bool,
    ) {
        for addr in addrs {
            if !streams.contains_key(addr) {
                streams.insert(
                    addr.to_string(),
                    BackendBuilder::from_with_response::<MemcacheResponseParser>(
                        addr.to_string(),
                        req,
                        resp,
                        parallel,
                        ignore,
                    ),
                );
            }
        }
    }

    fn update(&mut self, cfg: &str) {
        let (masters, followers, readers) = super::Config::from(cfg).into_split();

        if masters.len() == 0 {
            // TODO
            println!("parse cacheservice failed. master len is zero. cfg:{}", cfg);
            return;
        }

        self.masters = masters;
        self.followers = followers;
        self.readers = readers;

        let kb = 1024;
        let mb = 1024 * 1024;
        let p = 16;
        Self::delete_non_exists(&self.masters, &mut self.m_streams);
        Self::add_new(&self.masters, &mut self.m_streams, mb, kb, p, false);

        let followers: Vec<String> = self.followers.clone().into_iter().flatten().collect();
        Self::delete_non_exists(&followers, &mut self.f_streams);
        Self::add_new(followers.as_ref(), &mut self.f_streams, mb, kb, p, true);

        let readers: Vec<String> = self.readers.clone().into_iter().flatten().collect();
        // get command
        Self::delete_non_exists(&readers, &mut self.get_streams);
        Self::add_new(&readers, &mut self.get_streams, kb, mb, p, false);
        // get[s] command
        Self::delete_non_exists(&readers, &mut self.gets_streams);
        Self::add_new(&readers, &mut self.gets_streams, kb, mb, p, false);
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
    fn update(&mut self, cfg: &str) {
        self.update(cfg);
    }
}
impl left_right::Absorb<String> for Topology {
    fn absorb_first(&mut self, cfg: &mut String, _other: &Self) {
        self.update(cfg);
    }
    fn sync_with(&mut self, first: &Self) {
        *self = first.clone();
    }
}
