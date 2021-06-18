use serde::{Deserialize, Serialize};
use std::u64;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
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
    pub fn from(conf: &str) -> MemcacheConf {
        serde_yaml::from_str(conf).unwrap_or_else(|e| {
            println!(
                "parse cacheservice config failed. error:{:?} conf:{}",
                e, conf
            );
            Default::default()
        })
    }
    pub fn into_split(self) -> (Vec<String>, Vec<Vec<String>>, Vec<Vec<String>>) {
        let master = self.master;
        // followers包含： master-l1, slave, slave-l1
        let mut followers: Vec<Vec<String>> = self.master_l1.clone();
        followers.push(self.slave.clone());
        followers.extend(self.slave_l1);

        // reader包含：master，l1, slave
        // 保障严格的顺序。
        let mut readers = vec![master.clone()];
        readers.extend(self.master_l1);
        readers.push(self.slave);

        (master, followers, readers)
    }
}
