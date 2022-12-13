use std::collections::HashMap;

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

use axum::{
    extract::{ContentLengthLimit, Multipart, Path, Query},
    http::header::{HeaderMap, HeaderName, HeaderValue},
    http::StatusCode,
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use rand::prelude::random;
use serde::{Deserialize, Serialize};
use std::fs::read;
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

async fn path_req(Path(req): Path<Office>) -> String {
    format!(
        "worker:{},status:{},address:{}",
        req.worker, req.status, req.address
    )
}

async fn blog_struct() -> Json<Office> {
    let company = Office {
        worker: 20,
        status: "healthy".to_string(),
        address: "address".to_string(),
    };
    Json(company)
}

async fn query_req(Query(args): Query<OptionOffice>) -> String {
    format!(
        "worker:{},status:{},address:{}",
        args.worker.unwrap_or(16),
        args.status.unwrap_or("heaalthy".to_string()),
        args.address.unwrap_or_default(),
    )
}

async fn query(Query(params): Query<HashMap<String, String>>) -> String {
    // for (key, value) in &params {
    //     println!("key:{},value:{}", key, value);
    // }
    format!("{:?}", params)
}

// 上传表单
async fn show_upload() -> Html<&'static str> {
    Html(
        r#"
        <!doctype html>
        <html>
            <head>
            <meta charset="utf-8">
                <title>上传文件(仅支持图片上传)</title>
            </head>
            <body>
                <form action="/save_image" method="post" enctype="multipart/form-data">
                    <label>
                    上传文件(仅支持图片上传)：
                        <input type="file" name="file">
                    </label>
                    <button type="submit">上传文件</button>
                </form>
            </body>
        </html>
        "#,
    )
}

const SAVE_FILE_BASE_PATH: &str = "/Users/xinxin22/rustwebtest";
// 上传图片
async fn save_image(
    ContentLengthLimit(mut multipart): ContentLengthLimit<
        Multipart,
        {
            1024 * 1024 * 20 //20M
        },
    >,
) -> std::result::Result<(StatusCode, HeaderMap), String> {
    if let Some(file) = multipart.next_field().await.unwrap() {
        //文件类型
        let content_type = file.content_type().unwrap().to_string();

        //校验是否为图片(出于安全考虑)
        if content_type.starts_with("image/") {
            //根据文件类型生成随机文件名(出于安全考虑)
            let rnd = (random::<f32>() * 1000000000 as f32) as i32;
            //提取"/"的index位置
            let index = content_type
                .find("/")
                .map(|i| i)
                .unwrap_or(usize::max_value());
            //文件扩展名
            let mut ext_name = "xxx";
            if index != usize::max_value() {
                ext_name = &content_type[index + 1..];
            }
            //最终保存在服务器上的文件名
            let save_filename = format!("{}/{}.{}", SAVE_FILE_BASE_PATH, rnd, ext_name);

            //文件内容
            let data = file.bytes().await.unwrap();

            //辅助日志
            println!("filename:{},content_type:{}", save_filename, content_type);

            //保存上传的文件
            tokio::fs::write(&save_filename, &data)
                .await
                .map_err(|err| err.to_string())?;

            //上传成功后，显示上传后的图片
            return redirect(format!("/show_image/{}.{}", rnd, ext_name)).await;
        }
    }

    //正常情况，走不到这里来
    println!("{}", "没有上传文件或文件格式不对");

    //当上传的文件类型不对时，下面的重定向有时候会失败(感觉是axum的bug)
    return redirect(format!("/upload")).await;
}

/**
 * 显示图片
 */
async fn show_image(Path(id): Path<String>) -> (HeaderMap, Vec<u8>) {
    let index = id.find(".").map(|i| i).unwrap_or(usize::max_value());
    //文件扩展名
    let mut ext_name = "xxx";
    if index != usize::max_value() {
        ext_name = &id[index + 1..];
    }
    let content_type = format!("image/{}", ext_name);
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static("content-type"),
        HeaderValue::from_str(&content_type).unwrap(),
    );
    let file_name = format!("{}/{}", SAVE_FILE_BASE_PATH, id);
    (headers, read(&file_name).unwrap())
}

/**
 * 重定向
 */
async fn redirect(path: String) -> std::result::Result<(StatusCode, HeaderMap), String> {
    let mut headers = HeaderMap::new();
    //重设LOCATION，跳到新页面
    headers.insert(
        axum::http::header::LOCATION,
        HeaderValue::from_str(&path).unwrap(),
    );
    //302重定向
    Ok((StatusCode::FOUND, headers))
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "example_sse=debug,tower_http=debug")
    }
    tracing_subscriber::fmt::init();

    let app = Router::new()
        .route("/", get(|| async { "Hello, World!" }))
        //http://localhost:3000/path_req/20/rain/comp/
        .route("/path_req/:worker/:status/:address/", get(path_req))
        .route("/blog", get(blog_struct))
        // http://localhost:3000/query_req?worker=20&address=happyy
        .route("/query_req", get(query_req))
        //http://localhost:3000/query?worker=6&status=yin&address=base
        .route("/query", get(query))
        .route("/upload", get(show_upload))
        .route("/save_image", post(save_image))
        .route("/show_image/:id", get(show_image));

    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
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
