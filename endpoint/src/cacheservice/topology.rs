use stream::{BackendBuilder, Cid, RingBufferStream};

use protocol::memcache::MemcacheResponseParser;

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

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
    m_streams: HashMap<String, Arc<BackendBuilder>>,
    // 只用来同步写请求
    followers: Vec<Vec<String>>,
    f_streams: HashMap<String, Arc<BackendBuilder>>,
    // 处理读请求
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
    // 删除不存在的stream
    fn delete_non_exists(addrs: &[String], streams: &mut HashMap<String, Arc<BackendBuilder>>) {
        streams.retain(|addr, _| addrs.contains(addr));
    }
    // 添加新增的stream
    fn add_new(
        addrs: &[String],
        streams: &mut HashMap<String, Arc<BackendBuilder>>,
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
