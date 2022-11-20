use crate::hash::{init_pods, shard_check_with_files};
use sharding::distribution::Distribute;
use sharding::hash::Hasher;

const DISTS: [&str; 6] = [
    //"consistent",
    "modrange-1024",
    "modula",
    "range-256",
    "range-1024",
    "slotmod-1024",
    "splitmod-32",
];

const HASHES: [&str; 5] = [
    "crc32",
    "crc32-short",
    "crc32-pound",
    "crc32-num-5",
    "crc32-num",
    //"crc32-range-id",
    //"crc32_underscore",
];

const ROOT_PATH: &str = "./src/hash/records";

#[test]
fn crc32_test() {
    let shard_count = 8;
    let servers = init_pods(shard_count);
    for hash in HASHES {
        for dis in DISTS {
            let hasher = Hasher::from(hash);
            let dist = Distribute::from(dis, &servers);
            let path = format!("{}/{}_{}_", ROOT_PATH, hash, dis);
            let file_path = path.replace("-", "_");
            println!("path is :{}", file_path);
            shard_check_with_files(file_path, &hasher, &dist);
        }
    }
}


