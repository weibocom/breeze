use base64::{engine::general_purpose, Engine as _};
use serde::{de::Error, Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fs;

use crate::{Timeout, TO_MYSQL_M, TO_MYSQL_S};

//时间间隔，闭区间, 可以是2010, 或者2010-2015
#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Years(pub u16, pub u16);

impl<'de> Deserialize<'de> for Years {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match String::deserialize(deserializer) {
            Ok(interval) => {
                //暂时兼容defalut，配置更新后删除
                if interval == ARCHIVE_DEFAULT_KEY {
                    return Ok(Years(0, 0));
                }
                let mut interval_split = interval.split("-");
                let start = interval_split
                    .next()
                    .ok_or(Error::custom(&format!("interval is empty:{interval}")))?
                    .parse()
                    .map_err(Error::custom)?;
                let end = if let Some(end) = interval_split.next() {
                    end.parse().map_err(Error::custom)?
                } else {
                    start
                };
                Ok(Years(start, end))
            }
            Err(e) => Err(e),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct KvNamespace {
    #[serde(default)]
    pub(crate) basic: Basic,
    #[serde(skip)]
    pub(crate) backends_flaten: Vec<String>,
    #[serde(default)]
    pub(crate) backends: HashMap<Years, Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    listen: String,
    #[serde(default)]
    resource_type: String,
    #[serde(default)]
    pub(crate) selector: String,
    #[serde(default)]
    pub(crate) timeout_ms_master: u32,
    #[serde(default)]
    pub(crate) timeout_ms_slave: u32,
    #[serde(default)]
    pub(crate) db_name: String,
    #[serde(default)]
    pub(crate) table_postfix: String,
    #[serde(default)]
    pub(crate) db_count: u32,
    #[serde(default)]
    pub(crate) strategy: String,
    #[serde(default)]
    pub(crate) password: String,
    #[serde(default)]
    pub(crate) user: String,
    #[serde(default)]
    pub(crate) max_slave_conns: u16,
    #[serde(default)]
    pub(crate) region_enabled: bool,
}
pub const ARCHIVE_DEFAULT_KEY: &str = "__default__";

impl KvNamespace {
    #[inline]
    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        match serde_yaml::from_str::<KvNamespace>(cfg) {
            Ok(mut ns) => {
                //移除default分片，兼容老defalut
                ns.backends.remove(&Years(0, 0));
                //配置的年需要连续，不重叠
                let mut years: Vec<_> = ns.backends.keys().collect();
                if years.len() == 0 {
                    return None;
                }
                years.sort();
                let mut last_year = years[0].0 - 1;
                for year in years {
                    if year.0 > year.1 || year.0 != last_year + 1 {
                        return None;
                    }
                    last_year = year.1;
                }
                match ns.decrypt_password() {
                    Ok(password) => ns.basic.password = password,
                    Err(e) => {
                        log::warn!("failed to decrypt password, e:{}", e);
                        return None;
                    }
                }
                ns.backends_flaten = ns.backends.iter().fold(Vec::new(), |mut init, b| {
                    init.extend_from_slice(b.1);
                    init
                });
                Some(ns)
            }
            Err(e) => {
                log::info!("failed to parse mysql  e:{} config:{}", e, cfg);
                None
            }
        }
    }

    #[inline]
    fn decrypt_password(&self) -> Result<String, Box<dyn std::error::Error>> {
        let key_pem = fs::read_to_string(&context::get().key_path)?;
        let encrypted_data = general_purpose::STANDARD.decode(self.basic.password.as_bytes())?;
        let decrypted_data = ds::decrypt::decrypt_password(&key_pem, &encrypted_data)?;
        let decrypted_string = String::from_utf8(decrypted_data)?;
        Ok(decrypted_string)
    }
    pub(super) fn timeout_master(&self) -> Timeout {
        let mut to = TO_MYSQL_M;
        if self.basic.timeout_ms_master > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }
    pub(super) fn timeout_slave(&self) -> Timeout {
        let mut to = TO_MYSQL_S;
        if self.basic.timeout_ms_slave > 0 {
            to.adjust(self.basic.timeout_ms_slave);
        }
        to
    }
}
