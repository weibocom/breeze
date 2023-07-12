use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use sharding::{
    distribution::Distribute,
    hash::{Hash, Hasher},
};

#[test]
fn bkdrsub_one() {
    let hasher = Hasher::from("bkdrsub");

    let key1 = "mfh15d#3940964349989430";
    let hash1 = hasher.hash(&key1.as_bytes());
    assert_eq!(hash1, 2131766150);
    println!("key:{}, hash:{}, idx:{}", key1, hash1, hash1 % 180);
}

// TODO 临时批量文件的hash、dist校验测试，按需打开
//#[test]
fn bkdrsub_dist() {
    let path_base = "./redisKey";
    let port_start = 59152;
    let port_end = 59211;
    let shards = 180;

    let hasher = Hasher::from("bkdrsub");
    let servers = vec!["padding".to_string(); shards];
    let dist = Distribute::from("modula", &servers);
    let mut idx = 0;
    for p in port_start..port_end + 1 {
        let file = format!("{}/port_{}.txt", path_base, p);
        println!("will check bkdrsub file/{} with idx:{}", file, idx);
        check_file(idx, &file, hasher.clone(), dist.clone());
        idx += 1;
    }
}

/// 从文件中读取所有key，进行hash、dist后，check这些key是否都落在该idx上
fn check_file(idx: usize, file: &str, hasher: Hasher, dist: Distribute) {
    let f = File::open(file).expect(format!("bkdrsub file [{}] not exists!", file).as_str());
    let mut reader = BufReader::new(f);
    let mut count = 0;
    loop {
        let mut line = String::with_capacity(128);
        if let Ok(bytes) = reader.read_line(&mut line) {
            if bytes == 0 {
                // read EOF
                println!("bkdrsub file/{} processed {} lines", file, count);
                break;
            }
            // TODO 这些key的hash不配套
            if line.starts_with("pkm") || line.contains(":") {
                println!("ignore: {}", line);
                continue;
            }
            let line = line.trim().trim_end_matches(",");
            let hash = hasher.hash(&line.as_bytes());
            let idx_line = dist.index(hash);
            if idx_line != idx {
                println!("line:{}, hash:{}, idx:{}", line, hash, idx_line);
            }
            assert_eq!(
                idx, idx_line,
                "line[{}] dist err: {}/{}",
                line, idx, idx_line
            );
            count += 1;
        } else {
            break;
        }
    }
}
