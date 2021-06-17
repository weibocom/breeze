use serde::{Deserialize, Serialize};
use std::u64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MemcacheConf {
    pub hash: String,         // eg: bkdr
    pub distribution: String, //eg: ketama
    pub hash_tag: String,     //eg: user
    pub timeout: i32,         // unit: mills
    pub exptime: i64,
    pub master: Vec<String>,
    #[serde(default)]
    pub master_l1: Vec<Vec<String>>,
    #[serde(default)]
    pub slave: Vec<String>,
    #[serde(default)]
    pub slave_l1: Vec<Vec<String>>,
}

impl MemcacheConf {
    pub fn parse_conf(conf: &str) -> MemcacheConf {
        let mc: MemcacheConf = serde_yaml::from_str(conf).unwrap();
        return mc;
    }
}
