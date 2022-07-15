mod meta;
pub mod props;

use std::collections::HashMap;

use rocket::serde::json::Json;

use meta::{FileContent, Meta};

#[macro_use]
extern crate rocket;

#[macro_use]
extern crate lazy_static;

#[get("/hello")]
pub fn hello() -> &'static str {
    "Hello, fishermen!\r\n"
}

// 只支持：Content-Type: application/json
// 获得sockfile列表
#[get("/meta", format = "json")]
pub async fn meta_list() -> Json<Meta> {
    let mut meta = meta::Meta::from_evn();
    if let Err(e) = meta.load_sockfile_list().await {
        log::warn!("load sockfile list failed: {:?}", e);
    }
    Json(meta)
}

// 获取sockfile 或者 snapshot,
#[get("/meta/sockfile?<service>", format = "json")]
pub async fn sockfile_content(service: &str) -> Json<Vec<FileContent>> {
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

#[get("/meta/listener?<service>")]
pub async fn listener(service: &str) -> Json<HashMap<String, String>> {
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
