use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
// 按机器之间的距离来选择replica。
// 1. B类地址相同的最近与同一个zone的是近距离，优先访问。
// 2. 其他的只做错误处理时访问。
#[derive(Clone)]
pub struct Distance<T> {
    // batch: 至少在一个T上连续连续多少次
    batch_shift: u8,
    len_local: u16,
    seq: Arc<AtomicUsize>,
    pub(super) replicas: Vec<T>,
}
impl<T: Addr> Distance<T> {
    #[inline]
    pub fn from(mut replicas: Vec<T>) -> Self {
        let batch = 1024usize;
        debug_assert_ne!(replicas.len(), 0);
        // 把replica排序、分成3组: local、remote、跨region。
        let distances: HashMap<String, u16> = replicas
            .iter()
            .map(|t| (t.addr().to_string(), distance(t.addr())))
            .collect();
        replicas.sort_by(|a, b| distances[a.addr()].cmp(&distances[b.addr()]));
        // 至少取1/3的资源代码库local，1/3以后与local距离相同的也计算到local内
        let mut len_local = (replicas.len() / 3).max(1);
        let farest = distances[replicas[len_local - 1].addr()];
        for r in &replicas[len_local..] {
            if farest != distances[r.addr()] {
                break;
            }
            len_local += 1;
        }
        let seq = Arc::new(AtomicUsize::new(rand::random::<u16>() as usize));
        // 最小是1，最大是65536
        let batch_shift = batch.max(1).next_power_of_two().min(65536).trailing_zeros() as u8;
        log::info!(
            "local:{} {} {:?}",
            len_local,
            replicas.len(),
            replicas[0].addr()
        );
        Self {
            len_local: len_local as u16,
            replicas,
            batch_shift,
            seq,
        }
    }
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.replicas.len()
    }
    #[inline(always)]
    fn local_len(&self) -> usize {
        self.len_local as usize
    }
    #[inline(always)]
    fn remote_len(&self) -> usize {
        self.len() - self.local_len()
    }
    // 只从local获取
    #[inline(always)]
    pub unsafe fn unsafe_select(&self) -> (usize, &T) {
        debug_assert_ne!(self.len(), 0);
        let idx = if self.len() == 0 {
            0
        } else {
            (self.seq.fetch_add(1, Ordering::Relaxed) >> self.batch_shift as usize)
                % self.local_len()
        };
        (idx, self.replicas.get_unchecked(idx))
    }
    #[inline(always)]
    pub unsafe fn unsafe_next(&self, idx: usize, runs: usize) -> (usize, &T) {
        debug_assert!(runs < self.len());
        // 还可以从local中取
        let idx = if runs < self.local_len() {
            (idx + 1) % self.local_len()
        } else {
            debug_assert_ne!(self.local_len(), self.len());
            if idx == self.local_len() {
                // 从[idx_local..idx_remote)随机取一个
                self.seq.fetch_add(1, Ordering::Relaxed) % self.remote_len() + self.local_len()
            } else {
                if idx + 1 == self.len() {
                    self.local_len()
                } else {
                    idx + 1
                }
            }
        };
        (idx, self.replicas.get_unchecked(idx))
    }
}

fn distance(t: &str) -> u16 {
    DISTANCE_CALCULATOR
        .get()
        .expect("not init")
        .get()
        .distance(t)
}

pub trait Addr {
    fn addr(&self) -> &str;
}

impl<T> Addr for (String, T) {
    fn addr(&self) -> &str {
        &self.0
    }
}
use ds::{CowReadHandle, CowWriteHandle};
use once_cell::sync::OnceCell;
use std::sync::Mutex;
static DISTANCE_CALCULATOR: OnceCell<CowReadHandle<DistanceCalculator>> = OnceCell::new();
static DISTANCE_CALCULATOR_W: OnceCell<Mutex<CowWriteHandle<DistanceCalculator>>> = OnceCell::new();

type Address = String;
type Zone = String;
#[derive(Clone, Default, Debug)]
pub struct DistanceCalculator {
    local_b_class: String, // 本地的b类地址
    local_zone: String,
    // 不同zone之间的距离。
    distances: HashMap<Zone, HashMap<Zone, u16>>,
    zones: HashMap<Address, Zone>,
}

impl DistanceCalculator {
    fn distance(&self, addr: &str) -> u16 {
        let b_class = self.b_class(addr);
        if b_class == self.local_b_class {
            return 0;
        }
        if let Some(zone) = self.zones.get(b_class) {
            if let Some(distances) = self.distances.get(&self.local_zone) {
                if let Some(d) = distances.get(zone) {
                    return *d;
                }
            }
        }
        std::u16::MAX
    }
    // 获取B网段
    fn b_class<'a>(&self, addr: &'a str) -> &'a str {
        let mut dot_num = 0;
        for (idx, &b) in addr.as_bytes().iter().enumerate() {
            if b == b'.' {
                dot_num += 1;
                if dot_num == 2 {
                    return &addr[0..idx];
                }
            }
        }
        addr
    }
    fn refresh(&mut self, cfg: &str) {
        match serde_yaml::from_str::<IdcRegionCfg>(cfg) {
            Err(e) => log::info!("failed to parse distance calulator cfg:{:?}", e),
            Ok(cfg) => {
                let mut zones = HashMap::with_capacity(64);
                for one in cfg.relation.iter() {
                    let fields: Vec<&str> = one.split(",").collect();
                    if fields.len() == 3 {
                        let addr_b_class = fields[0].trim().to_string();
                        let zone = fields[1].trim().to_string();
                        // let idc = fields[2].trim().to_string();
                        zones.insert(addr_b_class, zone);
                    }
                }
                self.zones = zones;
                self.local_b_class = self.b_class(metrics::raw_local_ip()).to_string();
                if let Some(zone) = self.zones.get(&self.local_b_class) {
                    self.local_zone = zone.to_string();
                }

                // 通过优先级计算不同zone之间的距离
                let cap = cfg.priority.len();
                let mut distances = HashMap::with_capacity(cap);
                for one in cfg.priority {
                    let zones: Vec<&str> = one.split(",").collect();
                    let mut d = HashMap::with_capacity(cap);
                    for (i, z) in zones.iter().enumerate() {
                        // 同个B段是距离是0，同一个ZONE距离是1
                        d.insert(z.to_string(), 1 + i as u16);
                    }
                    distances.insert(zones[0].to_string(), d);
                }
                self.distances = distances;
                log::info!("zones:{:?}", self);
            }
        };
    }
}

pub fn build_refresh_idc() -> impl Fn(&str) {
    let (w, r) = ds::cow(DistanceCalculator::default());
    if let Err(_) = DISTANCE_CALCULATOR.set(r) {
        panic!("duplicate init");
    }
    if let Err(_) = DISTANCE_CALCULATOR_W.set(Mutex::from(w)) {
        panic!("duplicate init");
    }
    refresh_idc
}

fn refresh_idc(cfg: &str) {
    if let Ok(mut calculator) = DISTANCE_CALCULATOR_W.get().expect("not init").try_lock() {
        calculator.write(|t| t.refresh(cfg));
    }
}

use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct IdcRegionCfg {
    #[serde(default)]
    priority: Vec<String>,
    #[serde(default)]
    relation: Vec<String>,
}
