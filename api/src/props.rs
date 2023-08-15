// 使用env来存储kv属性变量
use ahash::{HashMap, HashMapExt};
use std::collections::HashSet;
use std::env;
use std::sync::RwLock;

lazy_static! {
    // 监听的socks
    static ref LISTENERS: RwLock<HashMap<String, String>> = RwLock::new(HashMap::with_capacity(32));
    // 可访问api的白名单
    static ref WHITELIST: RwLock<HashSet<String>> = RwLock::new(HashSet::with_capacity(2));
}

// 设置kv变量
pub fn set_prop(key: &str, val: &str) {
    if env::var(&key).is_err() {
        env::set_var(&key, val);
    }
}

// 获取变量，如果不存在返回默认值
pub fn get_prop(key: &str, default_val: &str) -> String {
    env::var(key).unwrap_or(default_val.to_string())
}

// 插入一条service的listener port，返回该service之前监听的port
pub fn add_listener(service: String, addr: String) -> Option<String> {
    log::info!("+++ add listener:{} - {}", service, addr);
    let mut listeners_w = LISTENERS.write().unwrap();
    (*listeners_w).insert(service, addr)
}

// 删除一条listener，返回service之前的监听端口
pub fn remove_listener(service: String) -> Option<String> {
    let mut listeners_w = LISTENERS.write().unwrap();
    (*listeners_w).remove(&service)
}

// 获得部分service的监听端口，如果services的长度为0，则返回全量数据
pub fn get_listeners(services: Vec<String>) -> HashMap<String, String> {
    let listeners_r = LISTENERS.read().unwrap();
    log::info!("+++ services:{}/{:?}", services.len(), services);
    if services.len() == 0 {
        log::info!("+++ all addrs:{:?}", *listeners_r);
        return (*listeners_r).clone();
    }
    let mut addrs = HashMap::with_capacity(services.len());
    for s in services {
        if let Some(addr) = (*listeners_r).get(&s) {
            addrs.insert(s, addr.clone());
        }
    }
    addrs
}

pub fn get_listener(service: &str) -> Option<String> {
    let listeners_r = LISTENERS.read().unwrap();
    if let Some(addr) = (*listeners_r).get(service) {
        return Some(addr.clone());
    }
    None
}

// 更新访问白名单
pub fn update_whitelist(whitelist: HashSet<String>) {
    let mut whitelist_w = WHITELIST.write().unwrap();
    log::info!("api whitelist {:?} => {:?}", whitelist_w, whitelist);

    whitelist_w.clear();
    whitelist_w.extend(whitelist);
}

// 是否是api访问的白名单
pub fn is_in_whitelist(ip: &String) -> bool {
    let whiltelist_r = WHITELIST.read().unwrap();
    whiltelist_r.contains(ip)
}
