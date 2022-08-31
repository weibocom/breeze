use serde::{Deserialize, Serialize};
use std::collections::HashMap;
pub trait ByDistance {
    // 按距离，取前 1 / div 个元素
    // 前freeze个不参与排序
    fn sort_and_take(&mut self, freeze: usize) -> usize;
}
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct IdcRegionCfg {
    #[serde(default)]
    region: HashMap<String, Vec<String>>,
    #[serde(default)]
    idc: HashMap<String, Vec<String>>,
    #[serde(default)]
    city: HashMap<String, Vec<String>>,
    #[serde(default)]
    twin: HashMap<String, Vec<String>>,
    #[serde(default)]
    neighbor: HashMap<String, Vec<String>>,
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
use ds::{CowReadHandle, CowWriteHandle};
use once_cell::sync::OnceCell;
use std::sync::Mutex;
static DISTANCE_CALCULATOR: OnceCell<CowReadHandle<DistanceCalculator>> = OnceCell::new();
static DISTANCE_CALCULATOR_W: OnceCell<Mutex<CowWriteHandle<DistanceCalculator>>> = OnceCell::new();
#[derive(Clone, Default, Debug)]
pub struct DistanceCalculator {
    local_idc: Option<String>, // 本地的b类地址
    local_region: Option<String>,
    local_city: Option<String>,
    local_neighbor: Option<String>,
    // 存储cidr与idc的映射关系
    idcs: HashMap<String, String>,
    // 存储idc与region的对应关系
    regions: HashMap<String, String>,
    // 存储region与城市的对应关系
    cities: HashMap<String, String>,
    // 有时候同一个idc物理距离上非常近，可以当作是同一个idc
    twins: HashMap<String, String>,
    neighbors: HashMap<String, String>,
}

impl DistanceCalculator {
    fn equal(&self, a: &Option<&String>, o: &Option<String>) -> bool {
        if let (Some(a), Some(o)) = (a, o) {
            a == &o
        } else {
            false
        }
    }
    // 相同的IDC，则距离为1
    // 相同的邻居，则距离为2
    // 相同的region则距离为4
    // 相同的城市，距离为8
    // 其他距离为u16::MAX
    fn distance(&self, addr: &str) -> u16 {
        let (idc, neighbor, region, city) = self.location(addr);
        if self.equal(&idc, &self.local_idc) {
            return 1;
        }
        if self.equal(&neighbor, &self.local_neighbor) {
            return 2;
        }
        if self.equal(&region, &self.local_region) {
            return 4;
        }
        if self.equal(&city, &self.local_city) {
            return 8;
        }
        u16::MAX
    }
    fn location(
        &self,
        addr: &str,
    ) -> (
        Option<&String>,
        Option<&String>,
        Option<&String>,
        Option<&String>,
    ) {
        let idc = self.idc(addr);
        let neighbor = idc.map(|idc| self.neighbors.get(idc)).flatten();
        let region = idc.map(|idc| self.regions.get(idc)).flatten();
        let city = region.map(|r| self.cities.get(r)).flatten();
        (idc, neighbor, region, city)
    }
    // 获取B网段
    fn idc(&self, addr: &str) -> Option<&String> {
        let mut addr = addr;
        while let Some(idx) = addr.rfind('.') {
            if idx == 0 {
                break;
            }
            addr = &addr[..idx];
            if let Some(idc) = self.idcs.get(addr) {
                return self.twins.get(idc).or(Some(idc));
            }
        }
        None
    }
    fn refresh(&mut self, cfg: &str) {
        match serde_yaml::from_str::<IdcRegionCfg>(cfg) {
            Err(e) => log::info!("failed to parse distance calulator cfg:{:?}", e),
            Ok(cfg) => {
                if cfg.idc.len() == 0 {
                    log::info!("no idc in distance calulator cfg");
                    return;
                }
                //use Flatten;
                self.idcs = cfg.idc.flatten();
                self.twins = cfg.twin.flatten();
                self.regions = cfg.region.flatten();
                self.cities = cfg.city.flatten();
                self.neighbors = cfg.neighbor.flatten();

                let (idc, neighbor, region, city) = self.location(metrics::raw_local_ip());
                let idc = idc.map(|s| s.to_string());
                let neighbor = neighbor.map(|s| s.to_string());
                let region = region.map(|s| s.to_string());
                let city = city.map(|s| s.to_string());
                self.local_idc = idc;
                self.local_neighbor = neighbor;
                self.local_region = region;
                self.local_city = city;

                log::info!("idc region refreshed:{:?}", self);
            }
        }
    }
}

pub trait Addr {
    fn addr(&self) -> &str;
    fn visit(&self, f: &mut dyn FnMut(&str)) {
        f(self.addr())
    }
}

impl<T> Addr for (String, T) {
    fn addr(&self) -> &str {
        &self.0
    }
}

impl<T, E> ByDistance for T
where
    T: std::ops::DerefMut<Target = [E]>,
    E: Addr,
{
    // 前freeze个不参与排序，排序原则：
    // 1. 距离最近的排前面
    // 2. 距离相同的，随机排序。避免可能的热点
    // 3. 排完充后，取前1/4个元素，以及与前1/4个元素最后一个距离相同的所有元素。
    fn sort_and_take(&mut self, freeze: usize) -> usize {
        let cal = unsafe { DISTANCE_CALCULATOR.get_unchecked().get() };
        let distances: HashMap<String, u16> = self
            .iter()
            .map(|a| {
                let (mut min, mut max) = (u16::MAX, 0);
                let mut addrs = Vec::new();
                a.visit(&mut |addr| {
                    let distance = cal.distance(addr);
                    min = min.min(distance);
                    max = max.max(distance);
                    addrs.push(addr.to_string());
                });
                // min 与 max差异过大，说明有问题
                if max - min >= 2 {
                    log::warn!("addr:{:?} distance is too large, {} {}", addrs, min, max);
                }
                (a.addr().to_string(), cal.distance(a.addr()))
            })
            .collect();
        let freeze = self.len().min(freeze);
        let (_pre, vals) = (&mut *self).split_at_mut(freeze);
        vals.sort_by(|a, b| {
            let da = distances[a.addr()];
            let db = distances[b.addr()];
            if da != db {
                da.cmp(&db)
            } else {
                // 随机排序
                (rand::random::<u32>() & 15).cmp(&8)
            }
        });
        let mut len_local = (self.len() / 4).max(1);
        let farest = distances[self[len_local - 1].addr()];
        for r in &self[len_local..] {
            if farest != distances[r.addr()] {
                break;
            }
            len_local += 1;
        }
        log::info!(
            "freeze:{}, len_local:{} total:{}, {:?}",
            freeze,
            len_local,
            self.len(),
            self.iter()
                .map(|a| (a.addr().to_string(), distances[a.addr()]))
                .collect::<Vec<_>>()
        );
        len_local
    }
}

trait Flatten {
    type Item;
    fn flatten(self) -> Self::Item;
}

impl Flatten for HashMap<String, Vec<String>> {
    type Item = HashMap<String, String>;
    fn flatten(self) -> Self::Item {
        self.into_iter()
            .map(|(k, v)| {
                v.into_iter()
                    .map(|e| (e, k.clone()))
                    .collect::<HashMap<String, String>>()
            })
            .flatten()
            .collect()
    }
}
