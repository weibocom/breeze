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
    idx_local: u16,
    idx_remote: u16,
    seq: Arc<AtomicUsize>,
    pub(super) replicas: Vec<T>,
}
impl<T: Addr> Distance<T> {
    #[inline]
    pub fn from(mut replicas: Vec<T>) -> Self {
        let batch = 1024usize;
        debug_assert_ne!(replicas.len(), 0);
        // 把replica排序、分成3组: local、remote、跨region。
        let distances: HashMap<String, u8> = replicas
            .iter()
            .map(|t| (t.addr().to_string(), distance(t.addr())))
            .collect();
        replicas.sort_by(|a, b| distances[a.addr()].cmp(&distances[b.addr()]));
        let mut idx_local = 0;
        let mut idx_remote = 0;
        for r in replicas.iter() {
            let d = distances[r.addr()];
            if d == LOCAL {
                idx_local += 1;
            }
            if d == REMOTE {
                idx_remote += 1;
            }
        }
        idx_remote += idx_local;
        if idx_local == 0 {
            idx_local = idx_remote;
        }
        let seq = Arc::new(AtomicUsize::new(rand::random::<u16>() as usize));
        // 最小是1，最大是65536
        let batch_shift = batch.max(1).next_power_of_two().min(65536).trailing_zeros() as u8;
        log::info!(
            "local:{} remote:{} first:{:?}",
            idx_local,
            idx_remote,
            replicas[0].addr()
        );
        Self {
            idx_local,
            idx_remote,
            replicas,
            batch_shift,
            seq,
        }
    }
    #[inline(always)]
    pub(super) fn len(&self) -> usize {
        self.idx_remote as usize
    }
    // 只从local获取
    #[inline(always)]
    pub unsafe fn unsafe_select(&self) -> (usize, &T) {
        debug_assert_ne!(self.len(), 0);
        let idx = (self.seq.fetch_add(1, Ordering::Relaxed) >> self.batch_shift as usize)
            % self.idx_local as usize;
        (idx, self.replicas.get_unchecked(idx))
    }
    #[inline(always)]
    pub unsafe fn unsafe_next(&self, idx: usize, runs: usize) -> (usize, &T) {
        debug_assert!(runs < self.len());
        // 还可以从local中取
        let idx = if runs < self.idx_local as usize {
            (idx + 1) % self.idx_local as usize
        } else {
            // 从remote中取
            (runs as usize) % self.len()
        };
        (idx, self.replicas.get_unchecked(idx))
    }
}

const LOCAL: u8 = 1;
const REMOTE: u8 = 2;
const FAR: u8 = 3;

// 0：local  （优先访问本地）
// 1: remote  (remote:本地异常后访问）
// 2: 异地。（无论如何都不访问）
fn distance(t: &str) -> u8 {
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
type City = String;
type Zone = String;
#[derive(Clone, Default, Debug)]
pub struct DistanceCalculator {
    local_city: String,
    local_zone: String,
    zones: HashMap<Address, (City, Zone)>,
}

impl DistanceCalculator {
    fn distance(&self, addr: &str) -> u8 {
        let addr = self.b_class(addr);
        match self.zones.get(addr) {
            None => REMOTE,
            Some((city, zone)) => {
                if self.local_city.len() > 0 && city.len() > 0 && &self.local_city != city {
                    FAR
                } else {
                    if zone == &self.local_zone {
                        LOCAL
                    } else {
                        REMOTE
                    }
                }
            }
        }
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
                    let fields: Vec<&str> = one.split(|c| c == ',' || c == '-').collect();
                    if fields.len() == 4 {
                        let addr_b_class = fields[0].trim().to_string();
                        let city = fields[1].trim().to_string();
                        let zone = fields[2].trim().to_string();
                        zones.insert(addr_b_class, (city, zone));
                    }
                }
                self.zones = zones;
                let local_addr = self.b_class(metrics::raw_local_ip());
                if let Some((city, zone)) = self.zones.get(local_addr) {
                    self.local_city = city.to_string();
                    self.local_zone = zone.to_string();
                }
                log::debug!("{} zones:{:?}", local_addr, self);
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
    relation: Vec<String>,
}
