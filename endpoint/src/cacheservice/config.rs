use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Namespace {
    #[serde(default)]
    pub hash: String, // eg: bkdr
    #[serde(default)]
    pub distribution: String, //eg: ketama
    #[serde(default)]
    pub hash_tag: String, //eg: user
    //pub timeout: i32,         // unit: mills
    //pub exptime: i64,
    #[serde(default)]
    pub master: Vec<String>,
    #[serde(default)]
    pub master_l1: Vec<Vec<String>>,
    #[serde(default)]
    pub slave: Vec<String>,
    #[serde(default)]
    pub slave_l1: Vec<Vec<String>>,
}

impl Namespace {
    pub(crate) fn parse(group_cfg: &str, namespace: &str) -> Result<Namespace> {
        log::debug!("group_cfg:{:?}", group_cfg);
        match serde_yaml::from_str::<HashMap<String, Namespace>>(group_cfg) {
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("parse cfg error:{:?}", e),
                ))
            }
            Ok(cs) => cs
                .into_iter()
                .find(|(k, _v)| k == namespace)
                .map(|(_k, v)| v)
                .ok_or(Error::new(
                    ErrorKind::NotFound,
                    format!("'{}' namespace not found", namespace),
                )),
        }
    }
}

impl Namespace {
    pub fn into_split(
        self,
    ) -> (
        Vec<String>,
        Vec<Vec<String>>,
        Vec<Vec<Vec<String>>>,
        String,
        String,
    ) {
        let master = self.master.clone();
        // followers包含： master-l1, slave, slave-l1
        let mut followers: Vec<Vec<String>> = self.master_l1.clone();
        if self.slave.len() > 0 {
            followers.push(self.slave.clone());
        }
        followers.extend(self.slave_l1.clone());

        // 保障严格的顺序:L1-master-slave; 同时将master也作为l1的一部分参与读取，使用者注意排重
        let mut readers = Vec::new();
        // readers: master-l1
        if self.master_l1.len() > 0 {
            let mut l1 = self.master_l1.clone();
            l1.push(self.master.clone());
            readers.push(l1);
        }
        // readers: master
        readers.push(vec![self.master.clone()]);
        // readers: slave
        if self.slave.len() > 0 {
            readers.push(vec![self.slave.clone()]);
        }

        log::debug!(
            "config parsed rs: master:{:?}, followers:{:?}, readers:{:?}",
            master,
            followers,
            readers
        );

        (master, followers, readers, self.hash, self.distribution)
    }
}
