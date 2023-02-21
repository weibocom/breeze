//! get /meta/snapshot
//! get /meta/sockfile
//! get /meta

#![allow(unused)]

use reqwest::blocking;
use serde::Deserialize;
// use std::collections::HashMap;

/// mete接口返回的数据：{"sock_path":"/home/thl/breezed/socks","snapshot_path":"/home/thl/breezed/snapshot","version":"gc49e797c_debug_vx","sockfiles":[...]}
/// 只测试sockfiles字段
/// 如果新增了socks，此处测试需要增加
/// 本地不要测试此用例
#[cfg(feature = "console-api")]
#[test]
fn meta() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct Resp {
        sockfiles: Vec<String>,
    }
    let mut resp: Resp = blocking::get("http://localhost:9984/breeze/meta")?.json()?;
    assert_eq!(
        resp.sockfiles.sort(),
        vec![
            "config+cloud+counterservice+testbreeze+meshtest@redis:9302@rs",
            "config+cloud+phantom+testbreeze+phantomtest@phantom:9303@pt",
            "config+cloud+redis+testbreeze+redismeshtest@redis:56810@rs",
            "config+cloud+redis+testbreeze+redismeshtestm@redis:56812@rs",
            "config+v1+cache.service.testbreeze.pool.yf+all:meshtest@mc:9301@cs"
        ]
        .sort()
    );
    Ok(())
}

#[cfg(feature = "console-api")]
#[test]
fn sockfile() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct Sock {
        name: String,
        content: String,
    }

    let mut url = reqwest::Url::parse("http://localhost:9984/breeze/meta/sockfile")?;
    url.query_pairs_mut()
        .append_pair("service", "config+cloud+redis+testbreeze+redismeshtest");

    let mut resp: Vec<Sock> = blocking::get(url)?.json()?;
    assert_eq!(
        resp[0].name,
        "config+cloud+redis+testbreeze+redismeshtest@redis:56810@rs"
    );
    Ok(())
}

// [{"name":"config+cloud+redis+testbreeze+redismeshtest","content":"FEgxAiVeCKxCmDCwTJ5xXQ 1669094903147435679\nbackends:\n- 10.182.27.228:56378,10.182.27.228:56378\n- 10.182.27.228:56379,10.182.27.228:56379\n- 10.182.27.228:56380,10.182.27.228:56380\n- 10.182.27.228:56381,10.182.27.228:56381\nbasic:\n  access_mod: rw\n  distribution: modula\n  hash: crc32local\n  listen: 56378,56379,56380,56381\n  resource_type: eredis\n  timeout_ms_master: 0\n  timeout_ms_slave: 0\n"}]
#[cfg(feature = "console-api")]
#[test]
fn snapshot() -> Result<(), Box<dyn std::error::Error>> {
    #[derive(Deserialize)]
    struct Sock {
        name: String,
        content: String,
    }

    let mut url = reqwest::Url::parse("http://localhost:9984/breeze/meta/snapshot")?;
    url.query_pairs_mut()
        .append_pair("service", "config+cloud+redis+testbreeze+redismeshtest");

    let mut resp: Vec<Sock> = blocking::get(url)?.json()?;
    assert_eq!(resp[0].name, "config+cloud+redis+testbreeze+redismeshtest");
    assert!(resp[0].content.contains("backends"));
    Ok(())
}
