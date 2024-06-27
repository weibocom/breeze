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
    pub(crate) backends_flaten: Vec<String>,
    #[serde(default)]
    pub(crate) backends: HashMap<Years, Vec<String>>,
    #[serde(default)]
    pub(crate) si_backends: Vec<String>,
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
    pub(crate) db_count: u32,
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
    #[serde(default)]
    pub(crate) si_db_name: String,
    #[serde(default)]
    pub(crate) si_table_name: String,
    #[serde(default)]
    pub(crate) si_db_count: u32,
    #[serde(default)]
    pub(crate) si_table_count: u32,
    #[serde(default)]
    pub(crate) si_user: String,
    #[serde(default)]
    pub(crate) si_password: String,
    #[serde(default)]
    pub(crate) si_cols: Vec<String>,
}

impl VectorNamespace {
    #[inline]
    pub(crate) fn try_from(cfg: &str) -> Option<Self> {
        match serde_yaml::from_str::<VectorNamespace>(cfg) {
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
                match ns.decrypt_password(ns.basic.password.as_bytes()) {
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
                if ns.basic.si_password.len() > 0 {
                    match ns.decrypt_password(ns.basic.si_password.as_bytes()) {
                        Ok(password) => ns.basic.si_password = password,
                        Err(e) => {
                            log::warn!("failed to decrypt si password, e:{}", e);
                            return None;
                        }
                    }
                }
                if ns.si_backends.len() > 0 {
                    ns.backends_flaten
                        .extend_from_slice(ns.si_backends.as_slice());
                }
                Some(ns)
            }
            Err(e) => {
                log::info!("failed to parse mysql  e:{} config:{}", e, cfg);
                None
            }
        }
    }

    #[inline]
    fn decrypt_password(&self, data: &[u8]) -> Result<String, Box<dyn std::error::Error>> {
        let key_pem = fs::read_to_string(&context::get().key_path)?;
        let encrypted_data = general_purpose::STANDARD.decode(data)?;
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
            to.adjust(self.basic.timeout_ms_slave);
        }
        to
    }
}
