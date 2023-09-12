use crate::ci::env::Mesh;
use std::io::prelude::*;
use std::net::TcpStream;
use std::time::Duration;

#[test]
#[ignore]
fn get() {
    let host_ip = "uuid".get_host();
    use std::thread;
    let n = 10;
    let mut handles = Vec::with_capacity(n);
    for i in 1..n + 1 {
        let host_ip = host_ip.clone();
        let handle = thread::spawn(move || {
            let mut stream = TcpStream::connect(host_ip).unwrap();
            for _ in 1..10 {
                for _ in 0..i {
                    stream.write_all(b"get biz\r\n").unwrap();
                }
                for _ in 0..i {
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
