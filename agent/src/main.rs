use ds::BrzMalloc;
#[global_allocator]
static GLOBAL: BrzMalloc = BrzMalloc {};

#[macro_use]
extern crate rocket;

mod console;
mod http;
mod prometheus;
mod service;
use discovery::*;
mod init;

use ds::time::Duration;
use rt::spawn;

use protocol::Result;

use serde::{Deserialize, Serialize};

use actix_web::{
    error, guard,
    http::{header::ContentType, StatusCode},
    web::{get, head, post, resource, scope, Data, Form, Json, Path, Query, ServiceConfig},
    App, Either, HttpRequest, HttpResponse, HttpServer, Responder,
};
use derive_more::{Display, Error};

#[derive(Debug, Display, Error)]
#[display(fmt = "my error: {}", name)]
pub struct MyError {
    name: &'static str,
}

// Use default implementation for `error_response()` method
impl error::ResponseError for MyError {}

// #[get("/")]
// async fn index() -> Result<&'static str, MyError> {
//    let err = MyError { name: "test error" };
//    info!("{}", err);
//    Err(err)
// }

#[derive(Debug, Serialize, Deserialize)]
struct Office {
    worker: i32,
    status: String,
    address: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct OptionOffice {
    worker: Option<i32>,
    status: Option<String>,
    address: Option<String>,
}

async fn hello() -> impl Responder {
    "Hello world!"
}

async fn path_req(data: Data<Office>) -> String {
    format!(
        "worker:{},status:{},address:{}",
        data.worker, data.status, data.address
    )
}

async fn show_office() -> Json<Office> {
    let company = Office {
        worker: 1,
        status: "healthy".to_string(),
        address: "sixA".to_string(),
    };
    Json(company)
}

//actix中配置不同路由可通过configure实现
fn scoped_config(cfg: &mut ServiceConfig) {
    cfg.service(
        resource("/test")
            .route(get().to(show_office))
            .route(head().to(|| HttpResponse::MethodNotAllowed())),
    );
}

fn config(cfg: &mut ServiceConfig) {
    cfg.service(
        resource("/app")
            .route(get().to(path_req))
            .route(head().to(|| HttpResponse::MethodNotAllowed())),
    );
}
// pub enum Hand<L, R> {
//     Left(L),
//     Right(R),
// }

// #[post("/")]
// fn index(form: Either<Json<Office>, Form<Office>>) -> String {
//     let name: String = match form {
//         Either::Left(json) => json.status.to_owned(),
//         Either::Right(form) => form.address.to_owned(),
//     };
//     format!("company {}!", name)
// }

async fn index(path: Path<(String, String)>, json: Json<Office>) -> impl Responder {
    let (name, count) = path.into_inner();
    format!("Welcome {}! {},{}", name, count, json.worker)
}

async fn query_req(args: Query<OptionOffice>) -> String {
    format!(
        "worker:{},status:{},address:{}",
        args.worker.unwrap_or(16).clone(),
        args.status.clone().unwrap_or("heaalthy".to_string()),
        args.address.clone().unwrap_or_default(),
    )
}

async fn post_json(company: Form<Office>) -> String {
    format!("Welcome {:?}!", company.into_inner())
}
// 默认支持
#[tokio::main]
async fn main() -> Result<()> {
    let company = Data::new(Office {
        worker: 20,
        status: "healthy".to_string(),
        address: "address".to_string(),
    });
    HttpServer::new(move || {
        App::new()
            //在注册数据之前创建一份数据
            .app_data(company.clone())
            .route("/path_req", get().to(path_req))
            .configure(config)
            //.route(post().to(path_req))
            .service(
                //scope设置特定应用前缀
                scope("/users")
                    .configure(scoped_config)
                    // .configure(scoped_config)
                    //.guard(guard::Header("user", "show"))
                    .route("/", get().to(hello))
                    .route("/show", get().to(show_office))
                    // /users/reso {"worker": 12,"status": "rain",address: "play"}
                    .service(resource("/reso").route(post().to(post_json))),
            )
            //反序列化路径 默认come black
            .route("/hello/{name}/{count}/{worker}", get().to(index))
            .route(
                "/query_req/{worker}/{status}/{address}",
                get().to(query_req),
            )
    })
    //.keep_alive(None)
    //.workers(context::get().thread_num as usize)
    .bind("127.0.0.1:4000")?
    .run()
    .await
    .unwrap();

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(context::get().thread_num as usize)
        .thread_name("breeze-w")
        .thread_stack_size(2 * 1024 * 1024)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { run().await })
}

async fn run() -> Result<()> {
    let ctx = context::get();
    init::init(ctx);

    // 将dns resolver的初始化放到外层，提前进行，避免并发场景下顺序错乱 fishermen
    let discovery = discovery::Discovery::from_url(&ctx.discovery);
    let (tx, rx) = ds::chan::bounded(128);
    let snapshot = ctx.snapshot_path.to_string();
    let tick = ctx.tick();
    let mut fix = discovery::Fixed::default();
    fix.register(ctx.idc_path_url(), discovery::distance::build_refresh_idc());

    // 从vintage获取socks
    if ctx.service_pool_socks_url().len() > 1 {
        fix.register(
            ctx.service_pool_socks_url(),
            discovery::socks::build_refresh_socks(ctx.service_path.clone()),
        );
    } else {
        log::info!(
            "only use socks from local path: {}",
            ctx.service_path.clone()
        );
    }

    rt::spawn(watch_discovery(snapshot, discovery, rx, tick, fix));
    log::info!("server inited {:?}", ctx);

    let mut listeners = ctx.listeners();
    listeners.remove_unix_sock().await?;
    loop {
        let (quards, failed) = listeners.scan().await;
        if failed > 0 {
            metrics::set_sockfile_failed(failed);
        }
        for quard in quards {
            let discovery = tx.clone();
            spawn(async move {
                match service::process_one(&quard, discovery).await {
                    Ok(_) => log::info!("service complete:{}", quard),
                    Err(_e) => log::warn!("service failed. {} err:{:?}", quard, _e),
                }
            });
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
