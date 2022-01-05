use std::path::Path;
use std::time::Duration;
#[derive(Debug, Clone)]
pub struct Quadruple {
    name: String,
    service: String,
    family: String,
    protocol: String,
    endpoint: String,
    addr: String,
}

// feed.content#yf@unix@memcache@cacheservice
impl Quadruple {
    // service@protocol@backend_type
    // service: 服务名称
    // protocol: 处理client连接的协议。memcache、redis等支持的协议.  格式为 mc:port.
    // backend_type: 后端资源的类型。是cacheservice、redis_dns等等
    pub(super) fn parse(name: &str) -> Option<Self> {
        let name = Path::new(name)
            .file_name()
            .map(|s| s.to_str())
            .unwrap_or(None)
            .unwrap_or("");
        let fields: Vec<&str> = name.split('@').collect();
        if fields.len() != 3 {
            log::warn!(
                "not a valid service file name:{}. must contains 4 fields seperated by '@'",
                name
            );
            return None;
        }
        let service = fields[0];
        let protocol_item = fields[1];
        let protocol_fields: Vec<&str> = protocol_item.split(':').collect();
        if protocol_fields.len() != 2 {
            log::warn!(
                "not a valid service file name::{} protocol {} must be splited by ':'",
                name,
                protocol_item
            );
            return None;
        }
        let protocol = protocol_fields[0];
        let family = "tcp";
        if let Err(e) = protocol_fields[1].parse::<u16>() {
            log::warn!(
                "not a valid service file name:{} not a valid port:{} error:{:?}",
                name,
                protocol_fields[1],
                e
            );
            return None;
        }
        #[cfg(feature = "listen-all")]
        let local_ip = "0.0.0.0";
        #[cfg(not(feature = "listen-all"))]
        let local_ip = "10.222.76.140";

        let addr = local_ip.to_string() + ":" + protocol_fields[1];

        let backend = fields[2];
        Some(Self {
            name: name.to_owned(),
            service: service.to_owned(),
            family: family.to_string(),
            protocol: protocol.to_owned(),
            endpoint: backend.to_string(),
            addr: addr.to_string(),
        })
    }
    pub fn name(&self) -> String {
        self.name.to_owned()
    }
    pub fn family(&self) -> String {
        self.family.to_owned()
    }
    pub fn address(&self) -> String {
        self.addr.to_owned()
    }
    pub fn protocol(&self) -> &str {
        &self.protocol
    }
    pub fn service(&self) -> &str {
        &self.service
    }
    // service的格式是 config+v1+breeze+feed.content.icy:user
    // 用'+'分隔的最后一个field是group:biz。取biz
    pub fn biz(&self) -> String {
        let fields: Vec<&str> = self.service.split(|c| c == '+' || c == ':').collect();
        fields.last().expect("biz").to_string()
    }
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
    // 从discovery同步数据的间隔周期
    pub fn tick(&self) -> Duration {
        Duration::from_secs(15)
    }
}

use std::fmt;
impl fmt::Display for Quadruple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "(name:{}, service:{}, prot:{}, addr:{}, endpoint:{})",
            self.name, self.service, self.protocol, self.addr, self.endpoint
        )
    }
}
