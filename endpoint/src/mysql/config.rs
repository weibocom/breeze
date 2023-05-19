use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use base64::{
    engine::general_purpose,
    Engine as _,
};
use openssl::pkey::{PKey};
use openssl::rsa::Padding;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct MysqlNamespace {
    // TODO speed up, ref: https://git/platform/resportal/-/issues/548
    #[serde(default)]
    pub(crate) basic: Basic,
    //backends_url 处理dns解析用
    #[serde(skip)]
    pub(crate) backends_url: Vec<String>,
    #[serde(default)]
    pub(crate) backends: HashMap<String, Vec<String>>,
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
    pub(crate) db_count: u32,
    #[serde(default)]
    pub(crate) strategy: String,
    #[serde(default)]
    pub(crate) password: String,
    #[serde(default)]
    pub(crate) user: String,
}
pub const ARCHIVE_DEFAULT_KEY: &str = "__default__";

impl MysqlNamespace {
    #[inline]
    pub(super) fn try_from(cfg: &str) -> Option<Self> {
        let nso = serde_yaml::from_str::<MysqlNamespace>(cfg)
            .map_err(|e| {
                log::info!("failed to parse mysql  e:{} config:{}", e, cfg);
                e
            })
            .ok();

        if let Some(mut ns) = nso {
            // archive shard 处理
            // 2009-2012 ,[111xxx.com:111,222xxx.com:222]
            // 2013 ,[112xxx.com:112,223xxx.com:223]
            let mut archive: HashMap<String, Vec<String>> = HashMap::new();
            for (key, mut val) in ns.backends.iter() {
                //处理当前库
                if ARCHIVE_DEFAULT_KEY == key {
                    archive.insert(key.to_string(), val.to_vec());
                    continue;
                }
                //适配N年共用一个组shard情况，例如2009-2012共用
                let years: Vec<&str> = key.split("-").collect();
                let min: u16 = years[0].parse().unwrap();
                if years.len() > 1 {
                    // 2009-2012 包括2012,故max需要加1
                    let max = years[1].parse::<u16>().expect("malformed mysql cfg") + 1_u16;
                    for i in min..max {
                        archive.insert(i.to_string(), val.to_vec());
                    }
                } else {
                    archive.insert(min.to_string(), val.to_vec());
                }
            }
            ns.backends = archive;
            //todo: 重复转化问题,待修改
            for vec in ns.backends.values() {
                ns.backends_url.extend(vec.iter().cloned());
            }

            // 解密
            if let Ok(password) = ns.decrypt_password() {
                ns.basic.password = password;
            }else {
                log::error!("failed to decrypt mysql password for {}:{}",ns.basic.user,ns.basic.db_name);
            }
            return Some(ns);
        }
        nso
    }

    #[inline]
    // 解密密码
    fn decrypt_password(&self) -> Result<String, Box<dyn std::error::Error>> {
        let key_pem = std::env::var("MYSQL_PRIVATE_KEY")?;
        let private_key = PKey::private_key_from_pem(key_pem.as_bytes())?;
        //将 base64 编码的字符串转换为字节数据
        let encrypted_data = general_purpose::STANDARD.decode(self.basic.password.as_bytes())?;
        let rsa = private_key.rsa()?;
        // 缓冲区大小必须 >= 密钥长度，目前使用2048位的密钥，所以缓冲区大小为256字节
        let mut decrypted_data = vec![0; rsa.size() as usize];
        rsa.private_decrypt(&encrypted_data, &mut decrypted_data, Padding::PKCS1)?;
        // 去除多余的0，只保留原始数据
        decrypted_data.retain(|&x| x != 0);
        Ok(String::from_utf8(decrypted_data).unwrap())
    }
}
