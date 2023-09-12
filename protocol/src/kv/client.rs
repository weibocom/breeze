use super::common::{
    constants::{CapabilityFlags, StatusFlags},
    opts::Opts,
};
use std::{collections::HashMap, ops::Deref, process};

#[derive(Default)]
pub struct Client {
    opts: Opts,
    pub capability_flags: CapabilityFlags,
    pub connection_id: u32,
    pub status_flags: StatusFlags,
    pub character_set: u8,
    pub server_version: Option<(u16, u16, u16)>,
}

impl Client {
    pub fn from_user_pwd(user: String, pwd: String) -> Self {
        Self {
            opts: Opts::from_user_pwd(user, pwd),
            ..Default::default()
        }
    }

    pub fn get_flags(&self) -> CapabilityFlags {
        let client_flags = CapabilityFlags::CLIENT_PROTOCOL_41
            | CapabilityFlags::CLIENT_SECURE_CONNECTION
            | CapabilityFlags::CLIENT_LONG_PASSWORD
            | CapabilityFlags::CLIENT_TRANSACTIONS
            | CapabilityFlags::CLIENT_LOCAL_FILES
            | CapabilityFlags::CLIENT_MULTI_STATEMENTS
            | CapabilityFlags::CLIENT_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PS_MULTI_RESULTS
            | CapabilityFlags::CLIENT_PLUGIN_AUTH
            | CapabilityFlags::CLIENT_CONNECT_ATTRS
            | CapabilityFlags::CLIENT_FOUND_ROWS;
        // 这个flag目前不会开启
        // | (self.capability_flags & CapabilityFlags::CLIENT_LONG_FLAG);
        // if self.0.opts.get_compress().is_some() {
        //     client_flags.insert(CapabilityFlags::CLIENT_COMPRESS);
        // }

        // 默认dbname 需要从config获取 fishermen
        // if let Some(db_name) = self.opts.get_db_name() {
        //     if !db_name.is_empty() {
        //         client_flags.insert(CapabilityFlags::CLIENT_CONNECT_WITH_DB);
        //     }
        // }

        // 暂时不支持ssl fishermen
        // if self.is_insecure() && self.0.opts.get_ssl_opts().is_some() {
        //     client_flags.insert(CapabilityFlags::CLIENT_SSL);
        // }

        client_flags | self.opts.get_additional_capabilities()
    }

    pub fn connect_attrs(&self) -> HashMap<String, String> {
        let program_name = match self.opts.get_connect_attrs().get("program_name") {
            Some(program_name) => program_name.clone(),
            None => {
                let arg0 = std::env::args_os().next();
                let arg0 = arg0.as_ref().map(|x| x.to_string_lossy());
                arg0.unwrap_or_else(|| "".into()).to_owned().to_string()
            }
        };

        let mut attrs = HashMap::new();

        attrs.insert("_client_name".into(), "mesh-mysql".into());
        attrs.insert("_client_version".into(), env!("CARGO_PKG_VERSION").into());
        // attrs.insert("_os".into(), env!("CARGO_CFG_TARGET_OS").into());
        attrs.insert("_pid".into(), process::id().to_string());
        // attrs.insert("_platform".into(), env!("CARGO_CFG_TARGET_ARCH").into());
        attrs.insert("program_name".into(), program_name);

        for (name, value) in self.opts.get_connect_attrs().clone() {
            attrs.insert(name, value);
        }

        attrs
    }
}

impl Deref for Client {
    type Target = Opts;

    fn deref(&self) -> &Self::Target {
        &self.opts
    }
}
