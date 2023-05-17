use reqwest::blocking::Client;
use std::fs::File;
use url::Url;

pub fn create(path: &str) {
    let (father, child) = path.rsplit_once('/').unwrap();

    let mut addr = Url::parse("http://127.0.0.1:8080").unwrap();
    addr.set_path(father);
    let c = Client::new();
    c.post(addr)
        .body(format!(r#"{{"name": "{child}"}}, "data":"data""#))
        .send()
        .expect("create vintage path failed");
}

pub fn update(path: &str, config: &str) {
    let mut addr = Url::parse("http://127.0.0.1:8080").unwrap();
    addr.set_path(path);
    let c = Client::new();
    c.put(addr)
        .body(format!(r#"{{"data": "{config}"}}"#))
        .send()
        .expect("create vintage config failed");
}

pub fn create_sock(name: &str) {
    let socks_dir = std::env::var("socks_dir").expect("env socks_dir not set");
    File::create(format!("{socks_dir}/{name}")).expect("create sock err");
}
