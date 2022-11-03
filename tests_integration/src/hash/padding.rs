use sharding::distribution::Distribute;
use sharding::hash::{Hash, Hasher};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};

const DISTS: [&str; 0] = [
    //"consistent",
    //"modrange",
    //"consistent",
    //"modrange",
    //"modula",
    //"padding",
    //"range",
    //"slotmod",
    //"splitmo",
];

const HASHES: [&str; 1] = ["padding"];

const ROOT_PATH: &str = "./src/hash/records";

#[test]
fn padding_test() {
    let shard_count = 8;
    let mut servers = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
        servers.push(format!("192.168.0.{}", i).to_string());
    }
    for hash in HASHES {
        for dis in DISTS {
            let hasher = Hasher::from(hash);
            let dist = Distribute::from(dis, &servers);
            //let h = hasher.hash(&"937529.WiC".as_bytes());
            //let idx = dist.index(h) as i64;
            //println!("index is :{}",idx);
            let file_name = hash.replace("-", "_");
            let path = format!("{}/{}{}", ROOT_PATH, file_name, "_");
            shard_check_with_files(path, &hasher, &dist);
        }
    }
}

fn shard_check_with_files(path: String, hasher: &Hasher, dist: &Distribute) {
    //shard_check_short_with_files(path.clone(), hasher, dist);

    let crc50 = format!("{}{}", path, "10.log");
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
