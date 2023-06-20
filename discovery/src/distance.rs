use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const DISTANCE_VAL_IDC: u16 = 1;
pub const DISTANCE_VAL_NEIGHBOR: u16 = 2;
pub const DISTANCE_VAL_REGION: u16 = 4;
pub const DISTANCE_VAL_CITY: u16 = 8;
pub const DISTANCE_VAL_OTHER: u16 = u16::MAX;

trait Distance {
    fn distance(&self) -> u16;
}
trait MinMax {
    fn min_max(&self) -> (u16, u16);
}
impl<T> Distance for T
where
    T: AsRef<str>,
{
    fn distance(&self) -> u16 {
        let cal = unsafe { DISTANCE_CALCULATOR.get_unchecked().get() };
        cal.distance(self.as_ref())
    }
}
impl<T> MinMax for T
where
    T: Addr,
{
    fn min_max(&self) -> (u16, u16) {
        let mut min = u16::MAX;
        let mut max = 0;
        self.visit(&mut |addr| {
            let d = addr.distance();
            if d < min {
                min = d;
            }
            if d > max {
                max = d;
            }
        });
        if max - min >= DISTANCE_VAL_NEIGHBOR {
            log::warn!(
                "{}-{} >= 2: distance too large => {}",
                min,
                max,
                self.string()
            );
        }
        (min, max)
    }
}

pub trait ByDistance {
    // 距离相同时，包含addrs的排序靠前
    fn sort<F: Fn(u16, usize) -> bool>(&mut self, first: Vec<String>, f: F) -> usize;
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
    fn equal(&self, a: &Option<&str>, o: &Option<String>) -> bool {
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
            return DISTANCE_VAL_IDC;
        }
        if self.equal(&neighbor, &self.local_neighbor) {
            return DISTANCE_VAL_NEIGHBOR;
        }
        if self.equal(&region, &self.local_region) {
            return DISTANCE_VAL_REGION;
        }
        if self.equal(&city, &self.local_city) {
            return DISTANCE_VAL_CITY;
        }
        DISTANCE_VAL_OTHER
    }
    fn location(&self, addr: &str) -> (Option<&str>, Option<&str>, Option<&str>, Option<&str>) {
        let idc = self.idc(addr);
        let as_str = String::as_str;
        let neighbor = idc.map(|idc| self.neighbors.get(idc)).flatten().map(as_str);
        let region = idc.map(|idc| self.regions.get(idc)).flatten().map(as_str);
        let city = region.map(|r| self.cities.get(r)).flatten().map(as_str);
        (idc, neighbor, region, city)
    }
    // 获取B网段
    fn idc(&self, addr: &str) -> Option<&str> {
        let mut addr = addr;
        while let Some(idx) = addr.rfind('.') {
            if idx == 0 {
                break;
            }
            addr = &addr[..idx];
            if let Some(idc) = self.idcs.get(addr) {
                return self.twins.get(idc).or(Some(idc)).map(|s| s.as_str());
            }
        }
        None
    }
    fn refresh(&mut self, cfg: &str) {
        match serde_yaml::from_str::<IdcRegionCfg>(cfg) {
            Err(_e) => log::info!("failed to parse distance calulator cfg:{:?}", _e),
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
    // m包含了地址的数量
    fn count(&self, m: &HashMap<&str, ()>) -> usize {
        let mut n = 0;
        self.visit(&mut |addr| {
            if m.contains_key(addr) {
                n += 1;
            }
        });
        n
    }
    fn string(&self) -> String {
        let mut s = String::new();
        self.visit(&mut |addr| {
            s.push_str(addr);
            s.push_str(",");
        });
        s
    }
}
impl<T: Addr, O> Addr for (T, O) {
    fn addr(&self) -> &str {
        self.0.addr()
    }
}
impl Addr for String {
    fn addr(&self) -> &str {
        self.as_str()
    }
}

impl Addr for Vec<String> {
    fn addr(&self) -> &str {
        if self.len() == 0 {
            ""
        } else {
            &self[0]
        }
    }
    fn visit(&self, f: &mut dyn FnMut(&str)) {
        for addr in self {
            f(addr);
        }
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
    // 3. 排完序后，按参数Fn(u16, usize)取local，此函数的功能是按(距离 和/或 实例数)决定当前addr是否入选local，参数说明：
    //   第1个参数是当前addr的距离
    //   第2个参数是当前addr在排序后vec里的序号
    fn sort<F: Fn(u16, usize) -> bool>(&mut self, first: Vec<String>, f: F) -> usize {
        let top: HashMap<&str, ()> = first.iter().map(|a| (a.as_str(), ())).collect();
        let distances: HashMap<String, u16> = self
            .iter()
            .map(|a| {
                let (_min, max) = a.min_max();
                (a.addr().to_string(), max)
            })
            .collect();
        self.sort_by(|a, b| {
            let da = distances[a.addr()];
            let db = distances[b.addr()];
            da.cmp(&db)
                .then_with(|| {
                    // 距离相同时地址包含在first中的数量越多，越优先。
                    let ca = a.count(&top);
                    let cb = b.count(&top);
                    cb.cmp(&ca)
                })
                // 距离相同时，随机排序。避免可能的热点
                .then_with(|| rand::random::<u16>().cmp(&(u16::MAX / 2)))
        });

        let mut len_local = 0;
        for (i, v) in self.iter().enumerate() {
            if f(distances[v.addr()], i) {
                len_local += 1;
            } else {
                break;
            }
        }
        if len_local > 0 {
            // 相同距离的也加到local
            let farest = distances[self[len_local - 1].addr()];
            for r in &self[len_local..] {
                if farest != distances[r.addr()] {
                    break;
                }
                len_local += 1;
            }
        }
        log::info!(
            "sort-by-distance.{} len_local:{} total:{}, {:?}",
            if first.len() > 0 {
                let new = self.get(0).map(|addr| {
                    let mut v = Vec::with_capacity(8);
                    addr.visit(&mut |a| {
                        v.push(a.to_string());
                    });
                    v
                });
                if Some(&first) != new.as_ref() {
                    format!("master changed. original:{}", first.join(","))
                } else {
                    String::new()
                }
            } else {
                String::new()
            },
            len_local,
            self.len(),
            self.iter()
                .map(|a| (a.string(), distances[a.addr()]))
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

pub trait Balance {
    fn balance(&mut self, first: &Vec<String>);
}

// 每一个分组，中的不同的shard，他们之间的距离可以差异比较大。
// 如果 [
//        ["4.44.1.1", "4.45.2.2"],
//        ["4.45.2.3", "4.44.1.2"]
//      ]
// 第一组中的 4.44.1.1与4.45.2.2的距离比较大，但与4.44.1.2的距离比较小。
// 因此可以进行一次balance，之后变成
//     [
//        ["4.44.1.1", "4.44.1.2"],
//        ["4.45.2.3","4.45.2.2"]
//      ]
//  满足以下要求：
//  0.  距离相同的不参与balance.
//  1.  只有相同的shard量的分组才能进行balance
//  2.  balance前后，节点在原有分组中的位置不能变化
//  3.  不同分组的顺序不保证变化
//  4. first中包含的元素排序靠前
impl Balance for Vec<Vec<String>> {
    fn balance(&mut self, first: &Vec<String>) {
        let top: HashMap<&str, ()> = first.iter().map(|a| (a.as_str(), ())).collect();
        let distances: HashMap<String, u16> = self
            .iter()
            .flatten()
            .map(|a| (a.clone(), a.distance()))
            .collect();

        let offs = self.split_off(0);
        let mut balancing = Vec::with_capacity(offs.len());
        self.reserve(offs.len());
        for e in offs {
            let (min, max) = e.min_max();
            if min == max {
                // 距离相同的，不需要balance
                self.push(e);
            } else {
                balancing.push(e);
            }
        }

        balancing.sort_by(|a, b| a.len().cmp(&b.len()));
        let mut by_len = std::collections::HashMap::new();
        for group in balancing {
            let one = by_len
                .entry(group.len())
                .or_insert_with(|| vec![Vec::new(); group.len()]);
            for (i, shard) in group.into_iter().enumerate() {
                one[i].push(shard);
            }
        }
        // 分别按不同的分组shard数量，对相同位置的shard进行排序。
        for (_, shards) in by_len.iter_mut() {
            for shard in shards {
                shard.sort_by(|a, b| {
                    // 按距离排序，距离相同按字母序
                    let da = distances[a];
                    let db = distances[b];
                    // 距离相同
                    // first包含
                    // 按b网络
                    da.cmp(&db)
                        .then_with(|| {
                            let ca = top.contains_key(a.as_str());
                            let cb = top.contains_key(b.as_str());
                            // 包括的排前面
                            cb.cmp(&ca)
                        })
                        .then_with(|| a.bclass().cmp(&b.bclass()))
                });
            }
        }
        let mut balanced = Vec::with_capacity(by_len.len());
        for (len, mut shards) in by_len {
            assert_ne!(len, 0);
            assert_eq!(shards.len(), len);
            let group_len = shards[0].len();
            // 一共有group_len个分组，每个分组有len个shard
            for _ in 0..group_len {
                let mut group = Vec::with_capacity(len);
                for j in 0..len {
                    group.push(shards[j].pop().expect("shard is empty"));
                }
                let (min, max) = group.min_max();
                if min == max {
                    balanced.push(group.clone());
                }
                self.push(group);
            }
        }
        if balanced.len() > 0 {
            log::info!("sort-by-distance: balanced: {:?}", balanced);
        }
    }
}

trait BClass {
    fn bclass(&self) -> u16;
}
// 获取ip地址的b段。用于排序
impl BClass for &String {
    fn bclass(&self) -> u16 {
        let mut ip = self.split('.');
        let mut b = 0u16;
        let mut idx = 0;
        while let Some(s) = ip.next() {
            idx += 1;
            b |= s.parse::<u8>().unwrap_or(u8::MAX) as u16;
            if idx == 2 {
                break;
            }
            b <<= 8;
        }
        b
    }
}
