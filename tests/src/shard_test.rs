//#![feature(map_first_last)]
use std::collections::BTreeMap;

use std::{
    fs::File,
    io::{BufRead, BufReader},
};

use ds::RingSlice;
use sharding::distribution::Distribute;
use sharding::hash::{Bkdr, Hash, Hasher};
use std::ops::Bound::Included;

#[test]
fn crc32_short() {
    // let key = "7516310920..uasvw";
    let key = "123测试中文hash.fri";

    let crc32_hasher = Hasher::from("crc32");
    let h = crc32_hasher.hash(&key.as_bytes());
    assert_eq!(h, 2968198350, "crc32 hash error");

    let crc32_short_hasher = Hasher::from("crc32-short");
    let h_short = crc32_short_hasher.hash(&key.as_bytes());
    assert_eq!(h_short, 12523, "crc32-short hash");

    let crc32_point = Hasher::from("crc32-point");
    let h_point = crc32_point.hash(&key.as_bytes());
    assert_eq!(h_point, 2642712869, "crc32-point hash");
}

fn build_servers(shard_count: usize) -> Vec<String> {
    // let shard_count = 8;
    let mut servers = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
        servers.push(format!("192.168.0.{}", i).to_string());
    }
    servers
}
#[allow(dead_code)]
fn root_path() -> &'static str {
    "./records"
}

//#[test]
//fn test_crc32_from_file() {
//    let root_path = root_path();
//    let dist = Distribute::from("modula", &build_servers());
//    let hasher = Hasher::from("crc32-short");
//    let path = format!("{}/crc32-short{}", root_path, "_");
//    shard_check_with_files(path, &hasher, &dist);
//
//    let hasher = Hasher::from("crc32-range");
//    let path = format!("{}/crc32-range{}", root_path, "_");
//    shard_check_with_files(path, &hasher, &dist);
//
//    let hasher = Hasher::from("crc32-range-id");
//    let path = format!("{}/crc32-range-id{}", root_path, "_");
//    shard_check_with_files(path, &hasher, &dist);
//
//    let hasher = Hasher::from("crc32-range-id-5");
//    let path = format!("{}/crc32-range-id-5{}", root_path, "_");
//    shard_check_with_files(path, &hasher, &dist);
//
//    let hasher = Hasher::from("crc32-range-point");
//    let path = format!("{}/crc32-range-point{}", root_path, "_");
//    shard_check_with_files(path, &hasher, &dist);
//}

#[test]
fn shards_check() {
    let root_path = "./sharding_datas/records";
    // will check crc32
    let shard_count = 8;
    let mut servers = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
        servers.push(format!("192.168.0.{}", i).to_string());
    }

    // 将java生成的随机key及hash，每种size都copy的几条过来，用于日常验证

    let check_bkdr = false;
    let check_crc32 = false;
    let check_crc32local = true;
    let check_consistant = false;

    // bkdr
    let hasher = Hasher::from("bkdr");
    let dist = Distribute::from("modula", &servers);
    if check_bkdr {
        let path = format!("{}/bkdr_", root_path);
        shard_check_with_files(path, &hasher, &dist);
    }

    // crc32-crc32local
    let dist = Distribute::from("range-256", &servers);

    // crc32
    if check_crc32 {
        let hasher = Hasher::from("crc32-short");
        let path = format!("{}/crc32-short{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);

        let hasher = Hasher::from("crc32-range");
        let path = format!("{}/crc32-range{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);

        let hasher = Hasher::from("crc32-range-id");
        let path = format!("{}/crc32-range-id{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);

        let hasher = Hasher::from("crc32-range-id-5");
        let path = format!("{}/crc32-range-id-5{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);

        let hasher = Hasher::from("crc32-range-point");
        let path = format!("{}/crc32-range-point{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);
    }

    // rawcrc32local
    if check_crc32local {
        let hasher = Hasher::from("rawcrc32local");
        let path = format!("{}/rawcrc32local{}", root_path, "_");
        // 只check长度不大于20的短key
        shard_check_short_with_files(path, &hasher, &dist);

        // crc32local
        let hasher = Hasher::from("crc32local");
        let path = format!("{}/crc32local{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);

        let hasher = Hasher::from("crc32local-point");
        let path = format!("{}/crc32local-point{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);

        let hasher = Hasher::from("crc32local-pound");
        let path = format!("{}/crc32local-pound{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);

        let hasher = Hasher::from("crc32local-underscore");
        let path = format!("{}/crc32local-underscore{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);
    }

    if check_consistant {
        let hasher = Hasher::from("crc32");
        let dist = Distribute::from("ketama", &servers);
        let path = format!("{}/crc32-consistant{}", root_path, "_");
        shard_check_with_files(path, &hasher, &dist);
    }
    // let consis10 = format!("{}{}", path, "consistent_10.log");
    // let consis15 = format!("{}{}", path, "consistent_15.log");
    // let consis20 = format!("{}{}", path, "consistent_20.log");
    // let consis50 = format!("{}{}", path, "consistent_50.log");
    // consistent_check(&consis10);
    // consistent_check(&consis15);
    // consistent_check(&consis20);
    // consistent_check(&consis50);

    let key = " 653017.hqfy";
    md5(&key);
}

// #[test]
// fn print_shards_check() {
//     let shard_count = 4;
//     let mut servers = Vec::with_capacity(shard_count);
//     for i in 0..shard_count {
//         servers.push(format!("192.168.0.{}", i).to_string());
//     }
//     let hasher = Hasher::from("crc32local");
//     let dist = Distribute::from("modula", &servers);

//     for i in 1..=20 {
//         let key = format!("test_shards_{}", i);
//         println!(
//             "{}: shards {}",
//             key,
//             dist.index(hasher.hash(&key.as_bytes()))
//         );
//     }
// }

fn shard_check_with_files(path: String, hasher: &Hasher, dist: &Distribute) {
    shard_check_short_with_files(path.clone(), hasher, dist);

    let crc50 = format!("{}{}", path, "50.log");
    shard_check(&crc50, &hasher, &dist);
}

// 不check 50长度的key
fn shard_check_short_with_files(path: String, hasher: &Hasher, dist: &Distribute) {
    let crc10 = format!("{}{}", path, "10.log");
    let crc15 = format!("{}{}", path, "15.log");
    let crc20 = format!("{}{}", path, "20.log");
    shard_check(&crc10, &hasher, &dist);
    shard_check(&crc15, &hasher, &dist);
    shard_check(&crc20, &hasher, &dist);
}

#[test]
fn layer_test() {
    let mut layer_readers = Vec::new();
    layer_readers.push(vec![vec!["1", "2", "3"], vec!["4", "5"], vec!["6", "7"]]);
    layer_readers.push(vec![vec!["1", "2", "3"], vec!["6", "7"]]);
    layer_readers.push(vec![vec!["7", "8"], vec!["4", "5"]]);

    let mut readers = Vec::new();
    for layer in &layer_readers {
        if layer.len() == 1 {
            let r = &layer[0];
            if !readers.contains(r) {
                readers.push(r.clone());
            }
        } else if layer.len() > 1 {
            for r in layer {
                if !readers.contains(r) {
                    readers.push(r.clone())
                }
            }
        }
    }

    assert!(readers.len() == 4);
    println!("readers: {:?}", readers);
}

#[test]
fn raw_hash() {
    let val1 = 123456789012;
    let key1 = format!("{}", val1);
    let key2 = format!("{}abc", val1);

    let hasher = Hasher::from("raw");
    let hash1 = hasher.hash(&key1[0..].as_bytes());
    let hash2 = hasher.hash(&key2[0..].as_bytes());
    println!("key:{}/{}, hash:{}/{}", key1, key2, hash1, hash2);
    assert_eq!(hash1, val1);
    assert_eq!(hash2, val1);
}

#[allow(dead_code)]
fn bkdr_check(path: &str) {
    let file = File::open(path).unwrap();
    let mut reader = BufReader::new(file);

    let mut num = 0;
    let shard_count = 8;
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
                // println!(
                //     "{} line: key: {}, hash: {}, hash-len:{}",
                //     num,
                //     key,
                //     hash_in_java,
                //     hash_in_java.len(),
                // );

                let idx = Bkdr {}.hash(&key.as_bytes()) % shard_count;
                let idx_in_java_u64 = idx_in_java.parse::<i64>().unwrap();
                if idx != idx_in_java_u64 {
                    println!(
                        "bkdr found error - line in java: {}, rust idx: {}",
                        line, idx
                    );
                    panic!("bkdr found error in bkdr");
                }
                // println!("hash in rust: {}, in java:{}", hash, hash_in_java);
            }
            Err(e) => {
                println!("Stop read for err: {}", e);
                break;
            }
        }
    }
    println!("check bkdr hash from file: {}", path);
}

#[test]
fn raw_check() {
    let hasher = Hasher::from("raw");
    let num = 123456789010i64;
    let key_str = format!("{}", num);
    let key = RingSlice::from(
        key_str.as_ptr(),
        key_str.capacity().next_power_of_two(),
        0,
        key_str.len(),
    );
    let hash = hasher.hash(&key);
    assert!(num == hash);
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

#[allow(dead_code)]
fn consistent_check(path: &str) {
    let file = File::open(path).unwrap();
    let mut reader = BufReader::new(file);

    let mut num = 0;
    let shards = vec![
        "127.0.0.1",
        "127.0.0.2",
        "127.0.0.3",
        "127.0.0.4",
        "127.0.0.5",
    ];
    let mut _instance = ConsistentHashInstance::from(shards);
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
                assert!(props.len() == 3);
                let key = props[0].trim();
                let hash_in_java = props[1].trim();
                let _server_in_java = props[2].trim();
                let hash_in_java_u64 = hash_in_java.parse::<i64>().unwrap();

                let (hash, _server) = _instance.get_hash_server(key);

                if hash != hash_in_java_u64 {
                    println!(
                        "consistent found error - line in java: {}, rust hash: {}",
                        line, hash
                    );
                    panic!("bkdr found error in crc32");
                }
                // println!(
                //     "crc32 succeed - key: {}, rust: {}, java: {}",
                //     key, hash_in_java, hash
                // );
            }
            Err(e) => println!("Stop read for err: {}", e),
        }
    }
    println!("check consistent hash from file: {}", path);
}

#[allow(dead_code)]
struct ConsistentHashInstance {
    consistent_map: BTreeMap<i64, usize>,
    shards: Vec<&'static str>,
}

impl ConsistentHashInstance {
    #[allow(dead_code)]
    fn from(shards: Vec<&'static str>) -> Self {
        let mut consistent_map = BTreeMap::new();
        for idx in 0..shards.len() {
            let factor = 40;
            for i in 0..factor {
                let data: String = shards[idx].to_string() + "-" + &i.to_string();
                let digest_bytes = md5::compute(data.as_str());
                for j in 0..4 {
                    let hash = (((digest_bytes[3 + j * 4] & 0xFF) as i64) << 24)
                        | (((digest_bytes[2 + j * 4] & 0xFF) as i64) << 16)
                        | (((digest_bytes[1 + j * 4] & 0xFF) as i64) << 8)
                        | ((digest_bytes[0 + j * 4] & 0xFF) as i64);

                    let mut hash = hash.wrapping_rem(i32::MAX as i64);
                    if hash < 0 {
                        hash = hash.wrapping_mul(-1);
                    }

                    consistent_map.insert(hash, idx);
                    //println!("+++++++ {} - {}", hash, shards[idx]);
                }
            }
        }
        //println!("map: {:?}", consistent_map);

        Self {
            shards,
            consistent_map,
        }
    }

    #[allow(dead_code)]
    fn get_hash_server(&self, key: &str) -> (i64, String) {
        // 一致性hash，选择hash环的第一个节点，不支持漂移，避免脏数据 fishermen
        let bk = Bkdr {};
        let h = bk.hash(&key.as_bytes()) as usize;
        let idxs = self
            .consistent_map
            .range((Included(h as i64), Included(i64::MAX)));

        for (hash, idx) in idxs {
            //println!("chose idx/{} with hash/{} for key/{}", idx, hash, key);
            let s = self.shards[*idx].to_string();
            return (*hash, s);
        }

        //if let Some(mut entry) = self.consistent_map.first_entry() {
        for (h, i) in &self.consistent_map {
            let s = self.shards[*i];
            return (*h, s.to_string());
        }

        return (0, "unavailable".to_string());
    }
}

fn md5(key: &str) {
    // let digest_str = md5.result_str();
    let out = md5::compute(key);
    // println!("key={}, md5={:?}", key, &out);

    // let digest_bytes = digest_str.as_bytes();
    for j in 0..4 {
        let hash = (((out[3 + j * 4] & 0xFF) as i64) << 24)
            | (((out[2 + j * 4] & 0xFF) as i64) << 16)
            | (((out[1 + j * 4] & 0xFF) as i64) << 8)
            | ((out[0 + j * 4] & 0xFF) as i64);

        let mut _hash = hash.wrapping_rem(i32::MAX as i64);
        if _hash < 0 {
            _hash = hash.wrapping_mul(-1);
        }
        // let hash_little = LittleEndian::read_i32(&out[j * 4..]) as i64;
        // let hash_big = BigEndian::read_i32(&out[j * 4..]) as i64;
        // println!("raw: {}  {}  {}", hash, hash_little, hash_big);
        // let hash = hash.wrapping_abs() as u64;

        //println!("+++++++ {} - {}", hash, j);
    }
}
#[test]
fn test_consis_sharding() {
    let servers = vec![
        "10.73.63.195:15010".to_string(),
        "10.13.192.12:15010".to_string(),
        "10.73.63.190:15010".to_string(),
        "10.73.63.188:15010".to_string(),
        "10.73.63.187:15010".to_string(),
    ];
    // let servers = servers.iter().map(|s| s.to_string()).collect();
    // let shard = sharding::Sharding::from("bkdr", "ketama", servers);
    let hasher = Hasher::from("bkdr");
    let dist = Distribute::from("ketama", &servers);

    let tuples = vec![
        ("4675657416578300.sdt", 1),
        ("4675358995775696.sdt", 3),
        ("4675216938370991.sdt", 3),
        ("4675342185268044.sdt", 2),
        ("4676725675655615.sdt", 0),
    ];
    for t in tuples {
        let hash = hasher.hash(&t.0.as_bytes());
        assert_eq!(dist.index(hash), t.1);
    }
}

#[test]
fn crc64_check() {
    crc64_file_check(6, "crc64_modula_6");
    crc64_file_check(8, "crc64_modula_8");
}

fn crc64_file_check(shards_count: usize, file_name: &str) {
    let root_dir = "sharding_datas/crc64";
    let shards = build_servers(shards_count);
    let shard_file = format!("{}/{}", root_dir, file_name);

    let file = File::open(shard_file).unwrap();
    let mut reader = BufReader::new(file);
    let mut success_count = 0;
    loop {
        let mut line = String::with_capacity(64);
        match reader.read_line(&mut line) {
            Ok(len) => {
                if len == 0 {
                    // println!("process file/{} completed!", file_name);
                    break;
                }
                line = line.trim().to_string();

                if line.trim().len() == 0 || line.starts_with("#") {
                    // println!("ignore comment: {}", line);
                    continue;
                }

                let fields: Vec<&str> = line.split("|").collect();
                if fields.len() != 3 {
                    // println!("malformed line:{}", line);
                    continue;
                }

                let hasher = Hasher::from("crc64");
                let dist = Distribute::from("modula", &shards);

                let key = fields[0];
                // c 的crc64的idx从1开始，需要减去1
                let idx_real = usize::from_str_radix(fields[1], 10).unwrap_or(usize::MAX) - 1;
                let hash = hasher.hash(&key.as_bytes());
                let idx = dist.index(hash);
                assert_eq!(
                    idx_real, idx,
                    "line: {} - {}:{}/{}",
                    line, hash, idx, idx_real
                );
                success_count += 1;

                // println!("proc succeed line:{}", line);
            }
            Err(e) => {
                println!("found err: {:?}", e);
                break;
            }
        }
    }
    println!("file:{}, succeed count:{}", file_name, success_count);
    assert!(success_count > 0);
}
