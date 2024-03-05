use base64::{engine::general_purpose, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;

pub use crate::kv::config::Years;
use crate::{Timeout, TO_VECTOR_M, TO_VECTOR_S};

#[derive(Debug, Clone, Default, Deserialize)]
pub struct VectorNamespace {
    #[serde(default)]
    pub(crate) basic: Basic,
    #[serde(skip)]
    pub(crate) backends_all: Vec<String>,
    #[serde(default)]
    pub(crate) backends: HashMap<Years, Vec<String>>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    pub(crate) resource_type: String,
    #[serde(default)]
    pub(crate) selector: String,
    #[serde(default)]
    pub(crate) timeout_ms_master: u32,
    #[serde(default)]
    pub(crate) timeout_ms_slave: u32,
    #[serde(default)]
    pub(crate) db_name: String,
    #[serde(default)]
    pub(crate) table_name: String,
    #[serde(default)]
    pub(crate) table_postfix: String,
    #[serde(default)]
    pub(crate) db_postfix: String,
    #[serde(default)]
    pub(crate) db_count: u32,
    #[serde(default)]
    pub(crate) table_count: u32,
    #[serde(default)]
    pub(crate) keys: Vec<String>,
    #[serde(default)]
    pub(crate) strategy: String,
    #[serde(default)]
    pub(crate) password: String,
    #[serde(default)]
    pub(crate) user: String,
    #[serde(default)]
    pub(crate) region_enabled: bool,
}

impl VectorNamespace {
    #[inline]
    pub(crate) fn try_from(cfg: &str) -> Option<Self> {
        match serde_yaml::from_str::<VectorNamespace>(cfg) {
            Ok(mut ns) => {
                //配置的年需要连续，不重叠，同时vector目前不支持default配置
                let mut years: Vec<_> = ns.backends.keys().collect();
                if years.len() == 0 || years[0].0 == 0 {
                    log::warn!("+++ malformed bk years:{}", cfg);
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
                ns.backends_all = ns.backends.iter().fold(Vec::new(), |mut init, b| {
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
    pub(crate) fn timeout_master(&self) -> Timeout {
        let mut to = TO_VECTOR_M;
        if self.basic.timeout_ms_master > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }
    pub(crate) fn timeout_slave(&self) -> Timeout {
        let mut to = TO_VECTOR_S;
        if self.basic.timeout_ms_slave > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }
}
