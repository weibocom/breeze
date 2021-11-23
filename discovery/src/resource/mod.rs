mod redis;
use std::collections::HashMap;

use protocol::Resource;
use redis::*;

pub(crate) async fn parse_cfg_hosts(
    resource: Resource,
    cfg: &String,
) -> HashMap<String, Vec<String>> {
    match resource {
        Resource::Memcache => return HashMap::with_capacity(0),
        Resource::Redis => match RedisNamespace::parse_hosts(cfg).await {
            Ok(hosts) => return hosts,
            Err(e) => {
                log::warn!("parse redis config failed: {:?}", e);
                return HashMap::with_capacity(0);
            }
        },
    }
}
