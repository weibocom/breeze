use ahash::{HashMap, HashMapExt};
use rocket::serde::json::Json;
use rocket::serde::Serialize;
use rocket::{Build, Rocket};
use std::io::{Error, ErrorKind};
use std::net::IpAddr;
use std::{io::Result, path::PathBuf};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::{props, verify_client};
use context::ListenerIter;

const PATH_META: &str = "meta";

#[derive(Debug, Default, Serialize)]
#[serde(crate = "rocket::serde")]
pub struct Meta {
    sock_path: String,
    snapshot_path: String,

    version: String,
    sockfiles: Vec<String>,
}

#[derive(Serialize)]
#[serde(crate = "rocket::serde")]
pub struct FileContent {
    name: String,
    content: String,
}

pub fn routes(rocket: Rocket<Build>) -> Rocket<Build> {
    rocket.mount(
        "/breeze",
        routes![meta_list, sockfile_content, snapshot_content, listener,],
    )
}

// 只支持：Content-Type: application/json
// 获得sockfile列表
#[get("/meta", format = "json")]
pub async fn meta_list(cip: IpAddr) -> Json<Meta> {
    // 校验client
    if !verify_client(&cip.to_string(), PATH_META) {
        return Json(Default::default());
    }

    let mut meta = Meta::from_evn();
    if let Err(_e) = meta.load_sockfile_list().await {
        log::warn!("load sockfile list failed: {:?}", _e);
    }
    Json(meta)
}

// 获取sockfile 或者 snapshot,
#[get("/meta/sockfile?<service>", format = "json")]
pub async fn sockfile_content(service: &str, cip: IpAddr) -> Json<Vec<FileContent>> {
    // 校验client
    if !verify_client(&cip.to_string(), PATH_META) {
        return Json(Vec::with_capacity(0));
    }

    let services: Vec<&str> = service.split(",").collect();
    let mut files = Vec::with_capacity(services.len());

    let meta = Meta::from_evn();
    for f in services {
        if let Ok(fc) = meta.sockfile(f).await {
            files.push(fc);
        } else {
            log::info!("not found sockfile for {}", f);
        }
    }

    Json(files)
}

//  snapshot,
#[get("/meta/snapshot?<service>", format = "json")]
pub async fn snapshot_content(service: &str, cip: IpAddr) -> Json<Vec<FileContent>> {
    // 校验client
    if !verify_client(&cip.to_string(), PATH_META) {
        return Json(Vec::with_capacity(0));
    }

    let services: Vec<&str> = service.split(",").collect();
    let mut files = Vec::with_capacity(services.len());

    let meta = Meta::from_evn();
    for f in services {
        if let Ok(fc) = meta.snapshot(f).await {
            files.push(fc);
        } else {
            log::info!("not found snapshot for {}", f);
        }
    }

    Json(files)
}

#[get("/meta/listener?<service>")]
pub async fn listener(service: &str, cip: IpAddr) -> Json<HashMap<String, String>> {
    // 校验client
    if !verify_client(&cip.to_string(), PATH_META) {
        return Json(HashMap::with_capacity(0));
    }

    let sparams: Vec<&str> = service.split(",").collect();
    let mut services = Vec::with_capacity(sparams.len());

    for p in sparams {
        let ptrim = p.trim();
        if ptrim.len() > 0 {
            services.push(ptrim.to_string());
        }
    }
    let listeners = props::get_listeners(services);
    log::info!("+++ get listeners:{:?}", listeners);
    Json(listeners)
}

impl Meta {
    fn from_evn() -> Self {
        let ctx = &context::get();
        Self {
            sock_path: ctx.service_path.clone(),
            snapshot_path: ctx.snapshot_path.clone(),

            version: ctx.version.to_string(),
            sockfiles: Default::default(),
        }
    }

    // 获取sock file的列表
    async fn load_sockfile_list(&mut self) -> Result<()> {
        let mut file_listener = ListenerIter::from(self.sock_path.clone());
        file_listener.remove_unix_sock().await?;

        let mut socks = Vec::with_capacity(20);
        let (quards, failed) = file_listener.scan().await;
        if failed > 0 {
            log::info!("api - found {} error sockfiles", failed);
        }
        for quad in quards {
            socks.push(quad.name());
        }
        self.sockfiles = socks;

        return Ok(());
    }

    pub async fn sockfile(&self, service: &str) -> Result<FileContent> {
        let mut file_listener = ListenerIter::from(self.sock_path.clone());
        file_listener.remove_unix_sock().await?;

        let (quards, failed) = file_listener.scan().await;
        if failed > 0 {
            log::info!(
                "api - found {} error sockfiles when read file content",
                failed
            );
        }
        for quad in quards {
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

    async fn snapshot(&self, service: &str) -> Result<FileContent> {
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
