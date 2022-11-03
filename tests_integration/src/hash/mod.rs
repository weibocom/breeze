use sharding::distribution::Distribute;
use sharding::hash::{Hash, Hasher};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};

mod bkdr;
mod crc32;
mod crc32local;
mod padding;
mod random;
mod raw;
mod rawcrc32local;
mod rawsuffix;

fn shard_check(path: &str, hasher: &Hasher, dist: &Distribute) {
    println!("will check file: {}", path);

    let file = File::open(path).unwrap();
    let mut reader = BufReader::new(file);

    let mut num = 0;
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(len) => {
                num += 1;
                if len == 0 {
                    println!("file processed copleted! all lines: {}", num);
                    break;
                }

                let props = line.split(" ").collect::<Vec<&str>>();
                assert!(props.len() == 2);
                let key = props[0].trim();
                let idx_in_java = props[1].trim();

                let h = hasher.hash(&key.as_bytes());
                let idx = dist.index(h) as i64;
                let idx_in_java_u64 = idx_in_java.parse::<i64>().unwrap();
                if idx != idx_in_java_u64 {
                    println!(
                        "{:?} found error - line in java: {}, rust idx: {}",
                        hasher, line, idx
                    );
                    panic!("crc32 check error");
                }
                // println!(
                //     "crc32 succeed - key: {}, rust: {}, java: {}",
                //     key, hash_in_java, hash
                // );
            }
            Err(e) => println!("Stop read for err: {}", e),
        }
    }
    println!("check crc32 from file: {} completed!", path);
}

fn init_pods(shard_count: usize) -> Vec<String> {
    let mut servers = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
        servers.push(format!("192.168.0.{}", i).to_string());
    }
    servers
}
