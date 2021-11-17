use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result};
use stream::LayerRole;

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
    pub(crate) fn new() -> Result<Namespace> {
        let new_one = Namespace {
            hash: "crc32".to_string(),
            distribution: "modula".to_string(),
            hash_tag: "".to_string(),
            master: vec!["10.185.13.228:56810".to_string(), "10.30.99.54:56811".to_string(), "10.13.224.231:56812".to_string()],
            master_l1: vec![vec!["10.185.19.150:56810".to_string(), "10.41.28.157:56810".to_string()],
                            vec!["10.13.113.201:56811".to_string(),"10.73.31.149:56811".to_string()],
                            vec!["10.41.11.223:56812".to_string(), "10.30.212.46:56812".to_string()]],
            slave: vec![],
            slave_l1: vec![]
        };
        Ok(new_one)
    }
    pub(crate) fn parse(group_cfg: &str, namespace: &str) -> Result<Namespace> {
        let test_cfg = "
basic:
  access_mod:rw
  hash: crc32
  distribution: modula
  listen: 56810,56811,56812
  resource_type: eredis
backends:
  - 10.185.13.228:56810,10.185.19.150:56810,10.41.28.157:56810
  - 10.30.99.54:56811,10.13.113.201:56811,10.73.31.149:56811
  - 10.13.224.231:56812,10.41.11.223:56812,10.30.212.46:56812";
        log::debug!("group_cfg:{:?}", group_cfg);
        log::debug!("group_cfg:{:?}", test_cfg);
        match serde_yaml::from_str::<HashMap<String, Namespace>>(test_cfg) {
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
    pub fn writers(&self) -> Vec<(LayerRole, Vec<String>)> {
        let mut w = Vec::with_capacity(8);
        if self.master.len() > 0 {
            w.push((LayerRole::Master, self.master.clone()));
            // w.extend(self.master_l1.clone());
            for ml1 in self.master_l1.iter() {
                w.push((LayerRole::MasterL1, ml1.clone()));
            }
            w.push((LayerRole::Slave, self.slave.clone()));
            // w.extend(self.slave_l1.clone());
            for sl1 in self.slave_l1.iter() {
                w.push((LayerRole::SlaveL1, sl1.clone()));
            }
        }
        w
    }
    pub fn uniq_all(&self) -> Vec<(LayerRole, Vec<String>)> {
        let mut all = vec![(LayerRole::Master, self.master.clone())];
        // all.extend(self.master_l1.clone());
        for ml1 in self.master_l1.iter() {
            all.push((LayerRole::MasterL1, ml1.clone()));
        }
        // all.extend(self.slave_l1.clone());
        for sl1 in self.slave_l1.iter() {
            all.push((LayerRole::SlaveL1, sl1.clone()));
        }
        all.push((LayerRole::Slave, self.slave.clone()));
        all
    }
}
