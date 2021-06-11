use stream::{BackendBuilder, Cid, MpscRingBufferStream};

use protocol::memcache::MemcacheResponseParser;

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tokio::net::tcp::OwnedWriteHalf;

type BackendStream = stream::BackendStream<Arc<MpscRingBufferStream>, Cid>;

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
    fn parse(_cfg: &str) -> (Vec<String>, Vec<Vec<String>>, Vec<Vec<String>>) {
        let masters = vec!["127.0.0.1:11211".to_string()];
        let followers = vec![vec![]];
        let readers = vec![vec!["127.0.0.1:11211".to_string()]];

        (masters, followers, readers)
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
        top
    }
}

impl discovery::Topology for Topology {
    fn copy_from(&self, cfg: &str) -> Self {
        self._copy(cfg)
    }
}
