use base64::{Engine as _, engine::general_purpose};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fmt::Debug, fs};

use crate::{TO_REDIS_M, TO_REDIS_S, Timeout};

// range/modrange 对应的distribution配置项如果有此后缀，不进行后端数量的校验
const NO_CHECK_SUFFIX: &str = "-nocheck";

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RedisNamespace {
    pub(crate) basic: Basic,
    pub(crate) backends: Vec<String>,
    // 对于一致性hash，为了确保ip变化后，分片不变，一般会为每组分片取一个name，来确定分片的hash始终固定
    #[serde(default)]
    pub(crate) backend_names: Vec<String>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Basic {
    #[serde(default)]
    pub(crate) access_mod: String,
    #[serde(default)]
    pub(crate) hash: String,
    #[serde(default)]
    pub(crate) distribution: String,
    //#[serde(default)]
    //pub(crate) listen: String,
    #[serde(default)]
    pub(crate) resource_type: String,
    #[serde(default = "RedisNamespace::default_selector")]
    pub(crate) selector: String,
    #[serde(default)]
    pub(crate) region_enabled: bool,
    #[serde(default)]
    pub(crate) timeout_ms_master: u32,
    #[serde(default)]
    pub(crate) timeout_ms_slave: u32,
    // master是否参与读
    #[serde(default)]
    pub(crate) master_read: bool,
    #[serde(default)]
    pub(crate) password: String,
}

impl RedisNamespace {
    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        let mut ns = serde_yaml::from_str::<RedisNamespace>(cfg)
            .map_err(|e| log::info!("failed to parse redis config:{} => {e:?}", cfg))
            .ok()?;
        if ns.backends.len() == 0 {
            log::warn!("cfg invalid:{:?}", ns);
            return None;
        }

        // check backends，分离出names
        let mut backends = Vec::with_capacity(ns.backends.len());
        for b in &mut ns.backends {
            let domain_name: Vec<&str> = b.split(" ").collect();
            // 后端地址格式： 域名,域名 name, name不能是rm、rs、','开头，避免把异常格式的slave当作name
            if domain_name.len() == 2
                && !domain_name[1].starts_with("rm")
                && !domain_name[1].starts_with("rs")
                && !domain_name[1].starts_with(",")
            {
                backends.push(domain_name[0].to_string());
                ns.backend_names.push(domain_name[1].to_string());
            }
        }
        if backends.len() > 0 {
            ns.backends = backends;
            log::info!("+++ found redis backends with name: {}", cfg);
        }

        if !ns.validate_and_correct() {
            log::error!("malformed names or shards {}: {}", ns.backends.len(), cfg);
            return None;
        }

        // 解密密码
        match ns.decrypt_password() {
            Ok(password) => ns.basic.password = password,
            Err(e) => {
                log::warn!("failed to decrypt password, e:{}", e);
                return None;
            }
        }

        log::debug!("parsed redis config:{}/{}", ns.basic.distribution, cfg);
        return Some(ns);
    }

    fn default_selector() -> String {
        "timeslice".to_string()
    }

    #[inline]
    pub(super) fn timeout_master(&self) -> Timeout {
        let mut to = TO_REDIS_M;
        if self.basic.timeout_ms_master > 0 {
            to.adjust(self.basic.timeout_ms_master);
        }
        to
    }

    #[inline]
    pub(super) fn timeout_slave(&self) -> Timeout {
        let mut to = TO_REDIS_S;
        if self.basic.timeout_ms_slave > 0 {
            to.adjust(self.basic.timeout_ms_slave);
        }
        to
    }

    /// 对配置进行合法性校验，当前只检验部分dist的后端数量
    #[inline(always)]
    fn validate_and_correct(&mut self) -> bool {
        let dist = &self.basic.distribution;

        // 需要检测dist时（默认场景），对于range/modrange类型的dist需要限制后端数量为2^n
        if dist.starts_with(sharding::distribution::DIST_RANGE)
            || dist.starts_with(sharding::distribution::DIST_MOD_RANGE)
        {
            // 对于range、morange，如果后有-nocheck后缀，不进行后端数量检测，并将该后缀清理掉
            if dist.ends_with(NO_CHECK_SUFFIX) {
                self.basic.distribution = dist.trim_end_matches(NO_CHECK_SUFFIX).to_string();
                return true;
            }
            let len = self.backends.len();
            let power_two = len > 0 && ((len & len - 1) == 0);
            if !power_two {
                return false;
            }
        }

        // 如果backend有name，则所有的后端都必须有name，且name不能重复
        if self.backend_names.len() > 0 {
            if self.backend_names.len() != self.backends.len() {
                return false;
            }
            let mut names_unique = HashSet::with_capacity(self.backend_names.len());
            names_unique.extend(self.backend_names.clone());
            if names_unique.len() != self.backend_names.len() {
                return false;
            }
        }

        true
    }

    #[inline]
    fn decrypt_password(&self) -> Result<String, Box<dyn std::error::Error>> {
        let key_pem = fs::read_to_string(&context::get().key_path)?;
        let encrypted_data = general_purpose::STANDARD.decode(self.basic.password.as_bytes())?;
        let decrypted_data = ds::decrypt::decrypt_password(&key_pem, &encrypted_data)?;
        let decrypted_string = String::from_utf8(decrypted_data)?;
        Ok(decrypted_string)
    }
}
