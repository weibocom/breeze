// 用于从vintage获取socks，并将socks文件在socks目录构建

use std::{
    collections::HashSet,
    fs::{self, OpenOptions},
};

use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Socks {
    #[serde(default)]
    socklist: HashSet<String>,

    #[serde(skip)]
    socks_path: String,

    // TODO：会议讨论的新逻辑：第一次成功更新后，不再从vintage更新，方便当前从本地随意调整socks，后续有需要再实时更新； fishermen 2023.2.10
    #[serde(default)]
    refreshed: bool,
}

pub static mut SOCKS: OnceCell<Socks> = OnceCell::new();

pub fn build_refresh_socks(socks_path: String) -> impl Fn(&str) {
    //todo delete socks dir
    unsafe {
        SOCKS.set(Socks::from(socks_path)).unwrap();
    }
    refresh_socks
}

//同时只有一个refresh_socks在运行
fn refresh_socks(cfg: &str) {
    unsafe {
        SOCKS.get_mut().unwrap().refresh(cfg);
    }
}

impl Socks {
    fn from(socks_path: String) -> Self {
        Self {
            socks_path,
            ..Default::default()
        }
    }
    fn refresh(&mut self, cfg: &str) {
        // 如果已经refreshed过了，暂时不再更新，后续有需要，重新打开
        if self.refreshed {
            log::debug!("+++ ignore socklist from vintage now!");
            return;
        }

        // 如果socklist为空，应该是刚启动，sockslist尝试从本地加载
        if self.socklist.len() == 0 {
            self.socklist = self.load_socks();
            log::info!("load dir/{} socks: {:?}", self.socks_path, self.socklist);
        }

        // 解析vintge配置，获得新的socklist
        match serde_yaml::from_str::<Socks>(cfg) {
            Ok(ns) => {
                if ns.socklist.len() == 0 {
                    log::info!("ignore refresh for socklist in vintage is empty");
                    return;
                }
                log::info!("will update socks to: {:?}", ns.socklist);
                // 先找到新上线的sock文件和下线的sock文件
                let mut deleted = Vec::with_capacity(2);
                for s in self.socklist.iter() {
                    if !ns.socklist.contains(s) {
                        deleted.push(s);
                    }
                }

                let mut added = Vec::with_capacity(2);
                for s in ns.socklist.iter() {
                    if !self.socklist.contains(s) {
                        added.push(s);
                    }
                }

                // 构建新上线的sock文件
                for n in added {
                    let fname = format!("{}/{}", self.socks_path, n);
                    match OpenOptions::new()
                        .write(true)
                        .create(true)
                        .open(fname.clone())
                    {
                        Ok(_f) => log::info!("create sock/{} succeed!", fname),
                        Err(_e) => log::warn!("create sock/{} failed: {:?}!", fname, _e),
                    }
                }

                // 删除下线的socks文件
                for d in deleted {
                    let fname = format!("{}/{}", self.socks_path, d);
                    match fs::remove_file(fname.clone()) {
                        Ok(_f) => log::info!("delete sock/{} succeed!", fname),
                        Err(_e) => log::warn!("delete sock/{} failed: {:?}", fname, _e),
                    }
                }

                // 设置刷新flag
                self.refreshed = true;
            }
            Err(_e) => log::warn!("parse socks file failed: {:?}, cfg:{} ", _e, cfg),
        }
    }

    fn load_socks(&self) -> HashSet<String> {
        // socks 目录必须存在且可读
        let mut socks = HashSet::with_capacity(8);
        let rdir = fs::read_dir(self.socks_path.clone()).unwrap();
        for entry in rdir.into_iter() {
            if let Ok(d) = entry {
                if let Ok(f) = d.file_type() {
                    if f.is_file() {
                        if let Some(fname) = d.file_name().to_str() {
                            socks.insert(fname.to_string());
                        }
                    }
                }
            }
        }
        socks
    }
}
