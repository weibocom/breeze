use rocket::serde::{json::Json, Serialize};
use std::io::{Error, ErrorKind};
use std::{io::Result, path::PathBuf};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::props;
use context::ListenerIter;

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Meta {
    base_path: String,
    sock_path: String,
    snapshot_path: String,
}

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
pub struct FileContent {
    name: String,
    content: String,
}

impl Meta {
    pub fn from_evn() -> Self {
        let base_path = props::from_evn("base_path".to_string(), "/data1/breeze/".to_string());
        let sock_path = format!("{}/{}", base_path, "socks");
        let snapshot_path = format!("{}/{}", base_path, "snapshot");
        log::info!(
            "+++ base path: {}, sock:{}, snapshot: {}",
            base_path,
            sock_path,
            snapshot_path
        );
        Self {
            base_path: base_path.to_string(),
            sock_path,
            snapshot_path,
        }
    }

    // 获取sock file的列表
    pub async fn sockfile_list(&mut self) -> Result<Vec<String>> {
        let mut file_listener = ListenerIter::from(self.sock_path.clone());
        file_listener.remove_unix_sock().await?;

        let mut socks = Vec::with_capacity(20);
        for quad in file_listener.scan().await {
            socks.push(quad.name());
        }

        return Ok(socks);
    }

    pub async fn sockfile(&self, service: &str) -> Result<FileContent> {
        let mut file_listener = ListenerIter::from(self.sock_path.clone());
        file_listener.remove_unix_sock().await?;
        for quad in file_listener.scan().await {
            if quad.service().eq(service) {
                let file_name = format!("{}/{}", self.sock_path, quad.name());
                let path_buf = PathBuf::from(file_name);
                let mut contents = Vec::with_capacity(8 * 1024);
                File::open(path_buf)
                    .await?
                    .read_to_end(&mut contents)
                    .await?;
                let contents = String::from_utf8(contents)
                    .map_err(|_e| Error::new(ErrorKind::Other, "not a valid utf8 file"))?;
                log::debug!("{} sockfile:{}", service, contents);
                let file = crate::meta::FileContent {
                    name: quad.name(),
                    content: contents,
                };
                return Ok(file);
            }
        }
        Err(Error::new(ErrorKind::InvalidInput, "not found file"))
    }

    pub async fn snapshot(&self, service: &str) -> Result<FileContent> {
        log::info!("+++ snapthshot path:{}", self.snapshot_path);
        let mut file_listener = ListenerIter::from(self.snapshot_path.clone());
        file_listener.remove_unix_sock().await?;
        for fp in file_listener.files().await? {
            log::info!("+++ compare fp:{}, service:{}", fp, service);
            if fp.eq(service) {
                let fpath = format!("{}/{}", self.snapshot_path, service);
                let path_buf = PathBuf::from(fpath);
                let mut contents = Vec::with_capacity(8 * 1024);
                File::open(path_buf)
                    .await?
                    .read_to_end(&mut contents)
                    .await?;
                let contents = String::from_utf8(contents)
                    .map_err(|_e| Error::new(ErrorKind::Other, "not a valid utf8 file"))?;
                log::debug!("{} snapshot loaded cfg:{}", service, contents);

                let file = crate::meta::FileContent {
                    name: service.to_string(),
                    content: contents,
                };
                return Ok(file);
            }
        }
        Err(Error::new(ErrorKind::InvalidInput, "not found file"))
    }
}
