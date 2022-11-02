use sharding::distribution::Distribute;
use sharding::hash::{Bkdr, Hash, Hasher};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};

#[test]
fn crc32_test_() {
    let shard_count = 8;
    let mut servers = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
        servers.push(format!("192.168.0.{}", i).to_string());
    }
    let dists = [
        "consistent",
        "modrange",
        "consistent",
        "modrange",
        "modula",
        "padding",
        "range",
        "slotmod",
        "splitmo",
    ];
    let root_path = "./records";
    for dis in dists {
        let hasher = Hasher::from("crc32-short");
        let dist = Distribute::from(dis, &servers);
        //let h = hasher.hash(&"1234.sda".as_bytes());
        //let idx = dist.index(h) as i64;
        let path = format!("{}/crc32-short{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);
    }
}

fn shard_check_with_files(path: String, hasher: &Hasher, dist: &Distribute) {
    shard_check_short_with_files(path.clone(), hasher, dist);

    let crc50 = format!("{}{}", path, "50.log");
    shard_check(&crc50, &hasher, &dist);
}

fn shard_check_short_with_files(path: String, hasher: &Hasher, dist: &Distribute) {
    let crc10 = format!("{}{}", path, "10.log");
    let crc15 = format!("{}{}", path, "15.log");
    let crc20 = format!("{}{}", path, "20.log");
    shard_check(&crc10, &hasher, &dist);
    shard_check(&crc15, &hasher, &dist);
    shard_check(&crc20, &hasher, &dist);
}

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
