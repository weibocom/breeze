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
    // 可写的实例。第一组一定是master. 包含f： master, master-l1, slave, slave-l1
    pub fn writers(&self) -> Vec<Vec<String>> {
        let mut w = Vec::with_capacity(8);
        if self.master.len() > 0 {
            w.push(self.master.clone());
            w.extend(self.master_l1.clone());
            w.push(self.slave.clone());
            w.extend(self.slave_l1.clone());
        }
        w
    }
}
