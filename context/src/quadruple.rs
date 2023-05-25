use ds::time::{Duration, Instant};
use std::path::Path;
#[derive(Debug, Clone, Eq)]
pub struct Quadruple {
    parsed_at: Instant,
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
    pub(super) fn parse(path: &str, name: &str) -> Option<Self> {
        let name = Path::new(name).file_name().map(std::ffi::OsStr::to_str)??;
        let fields: Vec<&str> = name.split('@').collect();
        if fields.len() != 3 {
            log::warn!("not a valid service:{}. 4 fields seperated by '@'", name);
            return None;
        }
        let service = fields[0];
        let protocol_item = fields[1];
        // 第一个field是应用协议名称(mc, redis)；
        // 第二个元素如果没有，则是unix协议，如果有则是tcp协议，该值必须是端口
        let protocol_fields: Vec<&str> = protocol_item.split(':').collect();
        let is_tcp = protocol_fields
            .get(1)
            .map(|port_str| port_str.parse::<u16>().is_ok())
            .unwrap_or(false);
        let (family, addr) = if is_tcp {
            #[cfg(feature = "listen-all")]
            let local_ip = "0.0.0.0";
            #[cfg(not(feature = "listen-all"))]
            let local_ip = "127.0.0.1";
            let addr = local_ip.to_string() + ":" + protocol_fields[1];
            ("tcp", addr)
        } else {
            (
                "unix",
                path.to_string() + "/" + protocol_fields.get(1).unwrap_or(&service) + ".sock",
            )
        };
        let mut protocol = protocol_fields[0];
        let mut backend = fields[2];
        // TODO 增加mysql的兼容逻辑，将mysql改为kv，待client修改上线后清理，预计2023.6.15后清理没问题
        const MYSQL: &str = "mysql";
        if MYSQL.eq(protocol) {
            protocol = "kv";
            backend = "kv";
            log::warn!("+++ found deprecated protocl: mysql:{}", name);
        }

        Some(Self {
            name: name.to_owned(),
            service: service.to_owned(),
            family: family.to_string(),
            protocol: protocol.to_owned(),
            endpoint: backend.to_string(),
            addr: addr.to_string(),
            parsed_at: Instant::now(),
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
            "({} => {}-{}://{} {:?})",
            self.service,
            self.protocol,
            self.endpoint,
            self.addr,
            self.parsed_at.elapsed(),
        )
    }
}

use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
impl PartialEq for Quadruple {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}
impl Ord for Quadruple {
    fn cmp(&self, other: &Self) -> Ordering {
        // unix < tcp
        other
            .family
            .cmp(&self.family)
            .then_with(|| self.name.cmp(&other.name))
    }
}
impl PartialOrd for Quadruple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
