mod meta;
pub mod props;

use std::io::Result;

use rocket::serde::{json::Json, Serialize};

use context::{Context, ListenerIter};
use meta::{FileContent, Meta};

#[macro_use]
extern crate rocket;

#[get("/hello")]
pub fn hello() -> &'static str {
    "Hello, fishermen!\r\n"
}

// 只支持：Content-Type: application/json
// 获得sockfile列表
#[get("/meta", format = "json")]
pub async fn meta_list() -> Json<Vec<String>> {
    let mut meta = meta::Meta::from_evn();
    match meta.sockfile_list().await {
        Ok(files) => {
            return Json(files);
        }
        Err(e) => return Json(vec![format!("err: {:?}", e)]),
    }
}

// 获取sockfile 或者 snapshot,
#[get("/meta/sockfile?<service>", format = "json")]
pub async fn sockfile_content(service: &str) -> Json<Vec<FileContent>> {
    log::info!("+++ services: {}", service);
    let services: Vec<&str> = service.split(",").collect();
    let mut files = Vec::with_capacity(services.len());
    let meta = meta::Meta::from_evn();
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
pub async fn snapshot_content(service: &str) -> Json<Vec<FileContent>> {
    log::info!("+++ services for snapshot: {}", service);
    let services: Vec<&str> = service.split(",").collect();
    let mut files = Vec::with_capacity(services.len());
    let meta = meta::Meta::from_evn();
    for f in services {
        if let Ok(fc) = meta.snapshot(f).await {
            files.push(fc);
        } else {
            log::info!("not found snapshot for {}", f);
        }
    }

    Json(files)
}
