use core::panic;
use std::{
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
};

use sharding::{
    distribution::Distribute,
    hash::{Hash, Hasher},
};

/// shard_checker 用于校验任何hash/distribution/shard_count 的正确性，校验数据的格式如下：
///   1. 首先设置好待check文件的header：
///       hash=bkdirsub
///       distribution=modula
///       shard_count=180
///   2. 为每一个分片的第一行记录分片idx，格式如：
///       shard_idx=0
///   3. 每个分片后续的行，记录该分片的key，格式如：
///       123456.abcd
///       456789.abcd
///
///   备注： 对分片的顺序无要求，但分片需要从0开始计数。

const HASH_PREIFX: &str = "hash=";
const DISTRIBUTION_PREIFX: &str = "distribution=";
const SHARD_COUNT_PREFIX: &str = "shard_count=";
const IDX_SHARD_PREFIX: &str = "shard_idx=";

/// 使用姿势很简单，按指定格式准备好分片文件，然后设置文件名，调用check_shard_data即可
#[test]
fn check_shard_data() {
    let root_dir = "sharding_datas/common";
    let data_file = "redis.data";

    shard_checker(root_dir, data_file);
}

#[test]
fn build_shard_data() {
    let shard_conf = ShardConf {
        hash: "crc32-point".to_string(),
        distribution: "secmod".to_string(),
        shards: 2,
    };

    let src = "sharding_datas/common/compare.txt";
    let dest = "/tmp/compare_shard.txt";

    write_shard_data(&shard_conf, src, dest);
}

/// shard校验的基础配置
#[derive(Debug, Default)]
struct ShardConf {
    hash: String,
    distribution: String,
    shards: usize,
}

impl ShardConf {
    /// check 是否parse 完毕
    fn parsed(&self) -> bool {
        if self.hash.len() == 0 || self.distribution.len() == 0 || self.shards == 0 {
            return false;
        }
        true
    }
}

fn parse_header(reader: &mut BufReader<File>) -> ShardConf {
    let mut shard_conf: ShardConf = Default::default();
    loop {
        let mut line = String::with_capacity(64);
        match reader.read_line(&mut line) {
            Ok(len) => {
                line = line.trim().to_string();
                // len 为0，说明读到文件末尾，退出loop
                if len == 0 {
                    println!("completed parse header！");
                    break;
                }

                // parse header
                if line.trim().len() == 0 || line.starts_with("#") {
                    // 读到空行或者注释行，跳过
                    // println!("ignoe line: {}", line);
                } else if line.starts_with(HASH_PREIFX) {
                    // 读到配置项，解析配置项
                    shard_conf.hash = line.split("=").nth(1).unwrap().trim().to_string();
                    // println!("hash: {}", shard_conf.hash);
                } else if line.starts_with(DISTRIBUTION_PREIFX) {
                    // 解析dist
                    shard_conf.distribution = line.split("=").nth(1).unwrap().trim().to_string();
                    // println!("distribution: {}", shard_conf.distribution);
                } else if line.starts_with(SHARD_COUNT_PREFIX) {
                    // 解析shard_count
                    shard_conf.shards = line.split("=").nth(1).unwrap().trim().parse().unwrap();
                    // println!("shards: {}", shard_conf.shards);
                }
                if shard_conf.parsed() {
                    break;
                }
            }
            Err(err) => {
                panic!("read file error: {}", err);
            }
        }
    }
    if shard_conf.hash.len() == 0 || shard_conf.distribution.len() == 0 || shard_conf.shards == 0 {
        panic!("parse header failed: {:?}", shard_conf);
    }
    shard_conf
}
fn shard_checker(root_dir: &str, data_file: &str) {
    // 首先读取文件header配置
    let data_file = format!("{}/{}", root_dir, data_file);
    let file = File::open(&data_file).unwrap();
    let mut reader = BufReader::new(file);

    let shard_conf = parse_header(&mut reader);
    println!("+++ hash file header: {:?}", shard_conf);

    // 做check的初始化：读取文件，初始化hash、dist等

    let shards = mock_servers(shard_conf.shards);
    let mut success_count = 0;
    let hasher = Hasher::from(shard_conf.hash.as_str());
    let dist = Distribute::from(shard_conf.distribution.as_str(), &shards);

    // 开始loop文件，check key
    let mut shard_idx_real = 0;
    loop {
        let mut line = String::with_capacity(64);
        match reader.read_line(&mut line) {
            Ok(len) => {
                // len 为0，说明读到文件末尾，停止loop
                if len == 0 {
                    println!("read all data");
                    break;
                }

                // 忽略空行和注释行
                line = line.trim().to_string();
                if line.len() == 0 || line.starts_with("#") {
                    // println!("ignore line:{}", line);
                    continue;
                }

                // 确认shard idx
                if line.starts_with(IDX_SHARD_PREFIX) {
                    shard_idx_real = line.split("=").nth(1).unwrap().parse::<usize>().unwrap();
                    // println!("will start check new shard: {}...", shard_idx_real);
                    continue;
                }

                // 把每行作为一个key，计算hash和dist
                let key = line;
                let hash = hasher.hash(&key.as_bytes());
                let idx = dist.index(hash);
                assert_eq!(
                    shard_idx_real, idx,
                    "key: {} - {}:{}, expected shard: {}",
                    key, hash, idx, shard_idx_real
                );
                success_count += 1;

                println!("proc succeed line:{}", key);
            }
            Err(e) => {
                println!("found err: {:?}", e);
                break;
            }
        }
    }
    println!("file:{}, succeed count:{}", data_file, success_count);
    assert!(success_count > 0);
}

fn mock_servers(shard_count: usize) -> Vec<String> {
    // let shard_count = 8;
    let mut servers = Vec::with_capacity(shard_count);
    for i in 0..shard_count {
        servers.push(format!("192.168.0.{}", i).to_string());
    }
    servers
}

fn write_shard_data(shard_conf: &ShardConf, src: &str, dest: &str) {
    let src_file = File::open(&src).unwrap();

    let mut writer = BufWriter::new(File::create(&dest).unwrap());
    let header = format!(
        "# 文件格式：首先设置hash、dist、shard_count，然后设置每个分片的数据\n# header\nhash={}\ndistribution={}\nshard_count={}\n",
        shard_conf.hash, shard_conf.distribution, shard_conf.shards
    );
    writer.write(header.as_bytes()).unwrap();

    // 计算每一行的key，记录到对应的分片位置
    let hasher = Hasher::from(&shard_conf.hash);
    let mock_servers = mock_servers(shard_conf.shards);
    let dist = Distribute::from(&shard_conf.distribution, &mock_servers);
    let mut shard_keys = Vec::new();
    for _i in 0..shard_conf.shards {
        shard_keys.push(Vec::with_capacity(128));
    }
    let mut reader = BufReader::new(src_file);
    loop {
        let mut line = String::with_capacity(64);
        match reader.read_line(&mut line) {
            Ok(len) => {
                if len == 0 {
                    // 读到文件末尾
                    break;
                }
                line = line.trim().to_string();
                let hash = hasher.hash(&line.as_bytes());
                let idx = dist.index(hash);
                shard_keys.get_mut(idx).unwrap().push(line);
            }
            Err(err) => {
                panic!("read error: {}", err);
            }
        }
    }

    // 将每个分片的key记录入目标文件
    for (i, keys) in shard_keys.into_iter().enumerate() {
        let shard_str = format!("\nshard_idx={}\n", i);
        writer.write_all(shard_str.as_bytes()).unwrap();
        for key in keys {
            writer.write(key.as_bytes()).unwrap();
            writer.write(b"\n").unwrap();
        }
        writer.flush().unwrap();
    }
}
