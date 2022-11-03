use crate::hash::shard_check;
use sharding::distribution::Distribute;
use sharding::hash::Hasher;

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

const HASHES: [&str; 1] = ["raw"];

const ROOT_PATH: &str = "./src/hash/records";

#[test]
fn raw_test() {
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
