use crate::ci::env::Mesh;
use std::io::prelude::*;
use std::net::TcpStream;
use std::time::Duration;

//以pipeline模式同时发送1-10个请求
#[test]
#[ignore]
fn get_with_pipeline() {
    let host_ip = "uuid".get_host();
    use std::thread;
    let n = 10;
    let mut handles = Vec::with_capacity(n);
    for i in 1..n + 1 {
        let host_ip = host_ip.clone();
        let handle = thread::spawn(move || {
            let mut stream = TcpStream::connect(host_ip).unwrap();
            for _ in 1..10 {
                //用于把请求
                let mut buf = Vec::new();
                for _ in 0..i {
                    buf.extend_from_slice(b"get biz\r\n");
                }
                stream.write_all(&buf).unwrap();
                for _ in 0..i {
                    //现在所有的成功响应都是40个字节
                    let mut buffer = [0; 40];
                    stream.read(&mut buffer).unwrap();
                    // println!("receive from {i} {response}");
                    let response = String::from_utf8_lossy(&buffer);
                    let lines: Vec<&str> = response.split("\r\n").collect();
                    assert!(lines.len() == 4);
                    lines[1].parse::<u64>().unwrap();
                }
                std::thread::sleep(Duration::from_secs(1));
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
}

//将一个请求拆开成两个tcp包发送
#[test]
#[ignore]
fn get_uncomplete_req() {
    let host_ip = "uuid".get_host();
    let host_ip = host_ip.clone();
    let mut stream = TcpStream::connect(host_ip).unwrap();
    let mut buf = Vec::new();
    const REQ_LEN: usize = 5;
    for _ in 0..REQ_LEN {
        buf.extend_from_slice(b"get biz\r\n");
    }
    for mid in 0..buf.len() {
        stream.write_all(&buf[0..mid]).unwrap();
        let _ = stream.flush();
        //尽量等待网卡发出
        std::thread::sleep(Duration::from_secs(1));
        stream.write_all(&buf[mid..buf.len()]).unwrap();
        for _ in 0..REQ_LEN {
            //现在所有的成功响应都是40个字节
            let mut buffer = [0; 40];
            stream.read(&mut buffer).unwrap();
            // println!("receive from {i} {response}");
            let response = String::from_utf8_lossy(&buffer);
            let lines: Vec<&str> = response.split("\r\n").collect();
            assert!(lines.len() == 4);
            lines[1].parse::<u64>().unwrap();
        }
    }
}
