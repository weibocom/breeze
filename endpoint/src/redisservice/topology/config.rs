use std::{
    collections::HashMap,
    io::{Error, ErrorKind, Result},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Namespace {
    pub basic: Basic,
    pub shards: Vec<Shard>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    pub access_mod: String,
    pub hash: String,
    pub distribution: String,
    pub listen: String,
    #[serde(default)]
    pub resource_type: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Shard {
    pub master: String,
    pub slave: String,
}

// impl Namespace {
//     pub fn from(basic: Basic, shards: Vec<Shard>) -> Self {
//         Self { basic, shards }
//     }
// }

impl Namespace {
    pub(crate) fn parse(group_cfg: &str, namespace: &str) -> Result<Namespace> {
        log::debug!("redis config/{}: {}", namespace, group_cfg);
        match serde_yaml::from_str::<HashMap<String, Namespace>>(group_cfg) {
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("parse cfg error: {:?}", e),
                ))
            }
            Ok(rs) => rs
                .into_iter()
                .find(|(k, _v)| k == namespace)
                .map(|(_k, v)| v)
                .ok_or(Error::new(
                    ErrorKind::NotFound,
                    format!("not found namespace:{}", namespace),
                )),
        }
    }

    pub(crate) fn master(&self) -> Vec<String> {
        let mut master = Vec::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            master.push(shard.master.clone());
        }
        master
    }

    pub(crate) fn slave(&self) -> Vec<String> {
        let mut slave = Vec::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            slave.push(shard.slave.clone());
        }
        slave
    }

    pub(crate) fn parse_listen_ports(&self) -> Vec<u16> {
        if self.basic.listen.len() == 0 {
            return Vec::with_capacity(0);
        }
        let ports: Vec<u16> = self
            .basic
            .listen
            .split(",")
            .map(|p| p.parse::<u16>().unwrap_or(0))
            .filter(|p| *p > 0u16)
            .collect();
        ports
    }
}
