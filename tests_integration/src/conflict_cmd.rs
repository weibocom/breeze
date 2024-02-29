use crate::ci::env::Mesh;
use memcache::MemcacheError;
use redis::RedisResult;
use std::{
    io::{Read, Write},
    net::TcpStream,
    time::Duration,
};

/// 根据资源提供的addr，进行连接，并发送对应协议的指令，用于验证mesh是否会panic
/// 不关注rsp，只要目标机器不panic，则认为验证通过

pub(crate) fn conflict_with_mc_cmd(origin_resource: &str) {
    // mc client建连会阻塞在version
    // let client = mc_helper::mc_get_conn(origin_resource);
    // let key = "fooadd";
    // let value = "bar";

    // println!("+++ before get...");
    // // 验证mc get cmd
    // let result: Result<Option<String>, MemcacheError> = client.get(key);
    // println!("{} use mc cmd: get rs: {:?}", origin_resource, result);
    // println!("+++ after get!");

    // // 验证mc add cmd
    // let rsp = client.add(key, value, 10);
    // println!("{} use mc cmd: get rs: {:?}", origin_resource, rsp);

    println!("will test with mc cmd...");
    let host_ip = origin_resource.get_host();

    let mut stream = TcpStream::connect(host_ip).unwrap();

    let req: Vec<u8> = vec![
        128, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 102, 111, 111,
        97, 100, 100,
    ];

    stream.write_all(req.as_slice()).unwrap();
    let mut buffer = [0; 40];
    stream.read(&mut buffer).unwrap();
    println!("{} use mc cmd get rs:{:?}", origin_resource, buffer);
}

/// 对mc发送redis指令，不能crash
pub(crate) fn conflict_with_redis_cmd(origin_resource: &str) {
    println!("{} will test with redis cmd", origin_resource);
    let mut conn = get_redis_conn(origin_resource);

    let key = "foo";
    let value = "bar";

    let rsp_get: RedisResult<String> = redis::cmd("GET").arg(key).query(&mut conn);
    println!("{} use redis cmd get:{:?}", origin_resource, rsp_get);

    let rsp_set: RedisResult<bool> = redis::cmd("SET").arg(key).arg(value).query(&mut conn);
    println!("{} use redis cmd set:{:?}", origin_resource, rsp_set);

    let rsp_del: RedisResult<bool> = redis::cmd("DEL").arg(key).query(&mut conn);
    println!("{} use redis cmd del:{:?}", origin_resource, rsp_del);
}

pub(crate) fn conflict_with_vector_cmd(origin_resource: &str) {
    let mut conn = get_redis_conn(origin_resource);

    let rsp: RedisResult<u64> = redis::cmd("vrange")
        .arg(format!("100,2211"))
        .arg("field")
        .arg("uid,object_type")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("1234")
        .arg("uid")
        .arg("=")
        .arg("5678")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut conn);

    println!("{} use vector vrange rsp:{:?}", origin_resource, rsp);
    assert!(rsp.err().is_some());

    let rsp: RedisResult<String> = redis::cmd("vrange")
        .arg("123,2211")
        .arg("field")
        .arg("uid,object_type,(select 1)")
        .arg("where")
        .arg("like_id")
        .arg("=")
        .arg("4968741184209240")
        .arg("limit")
        .arg("0")
        .arg("10")
        .query(&mut conn);
    println!("{} use vector vrange rsp:{:?}", origin_resource, rsp);
    assert!(rsp.is_err());
}

pub(crate) fn conflict_with_kv_cmd(origin_resource: &str) {
    // let client = crate::mc_helper::mc_get_conn(origin_resource);
    // let key = "123";

    // // 测试kv add cmd
    // let rsp = client.add(key, "1", 10000);
    // println!("{} use kv cmd: add rs: {:?}", origin_resource, rsp);

    // let result: Result<Option<String>, MemcacheError> = client.get(key);
    // println!("{} use kv cmd: get rs: {:?}", origin_resource, result);

    // // 测试kv get cmd
    // let result: Result<Option<String>, MemcacheError> = client.get(key);
    // println!("{} use kv cmd: set rs: {:?}", origin_resource, result);

    println!("will test with kv cmd...");
    let host_ip = origin_resource.get_host();

    let mut stream = TcpStream::connect(host_ip).unwrap();

    let req: Vec<u8> = vec![
        128, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 102, 111, 111,
        97, 100, 100,
    ];

    stream.write_all(req.as_slice()).unwrap();
    let mut buffer = [0; 40];
    stream.read(&mut buffer).unwrap();
    println!("{} use mc cmd get rs:{:?}", origin_resource, buffer);
}

pub(crate) fn conflict_with_uuid_cmd(origin_resource: &str) {
    let host_ip = origin_resource.get_host();

    let mut stream = TcpStream::connect(host_ip).unwrap();
    let count = 5;
    let mut buf = Vec::new();
    for _ in 0..count {
        buf.extend_from_slice(b"get biz\r\n");
    }
    stream.write_all(&buf).unwrap();
    for _ in 0..count {
        //现在所有的成功响应都是40个字节
        let mut buffer = [0; 40];
        stream.read(&mut buffer).unwrap();
        println!("{} user uuid cmd get rs:{:?}", origin_resource, buffer);
    }
}

fn get_redis_conn(origin_resource: &str) -> redis::Connection {
    use redis::Client;
    let host = origin_resource.get_host();
    let hoststr = String::from("redis://") + &host;
    let client = Client::open(hoststr).expect("get client err");
    client
        .get_connection_with_timeout(Duration::new(1, 0))
        .expect(format!("connected failed to {}", host).as_str())
}
