use crate::hash::{init_pods, shard_check_with_files};
use sharding::distribution::Distribute;
use sharding::hash::Hasher;

const DISTS: [&str; 1] = [
    //"consistent",
    //"modrange",
    //"consistent",
    //"modrange",
    "modula",
    //"padding",
    //"range",
    //"slotmod",
    //"splitmo",
];

const HASHES: [&str; 1] = ["bkdr"];

const ROOT_PATH: &str = "./src/hash/records";

#[test]
fn bkdr_test() {
    let shard_count = 8;
    let servers = init_pods(shard_count);

    for hash in HASHES {
        for dis in DISTS {
            let hasher = Hasher::from(hash);
            let dist = Distribute::from(dis, &servers);
            //let h = hasher.hash(&"937529.WiC".as_bytes());
            //let idx = dist.index(h) as i64;
            //println!("index is :{}",idx);
            let file_name = hash.replace("-", "_");
            let path = format!("{}/{}{}", ROOT_PATH, file_name, "_");
            println!("path is :{}", path);
            shard_check_with_files(path, &hasher, &dist);
        }
    }
}
