#[cfg(test)]
mod shard_test {
    //#![feature(map_first_last)]
    use std::collections::BTreeMap;

    use std::{
        fs::File,
        io::{BufRead, BufReader},
    };

    use crypto::digest::Digest;
    use crypto::md5::Md5;
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
        println!("=== crc32 hash: {}", h);

        let crc32_short_hasher = Hasher::from("crc32-short");
        let h_short = crc32_short_hasher.hash(&key.as_bytes());
        println!("crc32-short hash:{}", h_short);

        let crc32_point = Hasher::from("crc32-point");
        let h_point = crc32_point.hash(&key.as_bytes());
        println!("crc32-point hash: {}", h_point);

        // let i = key.find(".").unwrap();
        // let i2 = key.as_bytes().utf8_find('.').unwrap();
        // use ds::*;
        // let slice = RingSlice::from(key.as_ptr(), key.len().next_power_of_two(), 0, key.len());
        // let i3 = slice.utf8_find('.').unwrap();
        // println!("{}'s delimiter pos:{}/{}/{}", key, i, i2, i3);
    }

    #[test]
    fn shard_check() {
        // check raw hash
        raw_check();

        // 将java生成的随机key及hash，每种size都copy的几条过来，用于日常验证
        let path = "./records/";
        let bkdr10 = format!("{}{}", path, "bkdr_10.log");
        let bkdr15 = format!("{}{}", path, "bkdr_15.log");
        let bkdr20 = format!("{}{}", path, "bkdr_20.log");
        let bkdr50 = format!("{}{}", path, "bkdr_50.log");
        bkdr_check(&bkdr10);
        bkdr_check(&bkdr15);
        bkdr_check(&bkdr20);
        bkdr_check(&bkdr50);

        // will check crc32
        let shard_count = 8;
        let mut servers = Vec::with_capacity(shard_count);
        for i in 0..shard_count {
            servers.push(format!("192.168.0.{}", i).to_string());
        }
        let dist = Distribute::from("range-256", &servers);

        let hasher = Hasher::from("crc32-short");
        let crc10 = format!("{}{}", path, "crc32_short_10.log");
        let crc15 = format!("{}{}", path, "crc32_short_15.log");
        let crc20 = format!("{}{}", path, "crc32_short_20.log");
        let crc50 = format!("{}{}", path, "crc32_short_50.log");
        crc32_check(&crc10, &hasher, &dist);
        crc32_check(&crc15, &hasher, &dist);
        crc32_check(&crc20, &hasher, &dist);
        crc32_check(&crc50, &hasher, &dist);

        let hasher = Hasher::from("crc32-range");
        let crc10 = format!("{}{}", path, "crc32_range_10.log");
        let crc15 = format!("{}{}", path, "crc32_range_15.log");
        let crc20 = format!("{}{}", path, "crc32_range_20.log");
        let crc50 = format!("{}{}", path, "crc32_range_50.log");
        crc32_check(&crc10, &hasher, &dist);
        crc32_check(&crc15, &hasher, &dist);
        crc32_check(&crc20, &hasher, &dist);
        crc32_check(&crc50, &hasher, &dist);

        let hasher = Hasher::from("crc32-range-id");
        let crc10 = format!("{}{}", path, "crc32_range_id_10.log");
        let crc15 = format!("{}{}", path, "crc32_range_id_15.log");
        let crc20 = format!("{}{}", path, "crc32_range_id_20.log");
        let crc50 = format!("{}{}", path, "crc32_range_id_50.log");
        crc32_check(&crc10, &hasher, &dist);
        crc32_check(&crc15, &hasher, &dist);
        crc32_check(&crc20, &hasher, &dist);
        crc32_check(&crc50, &hasher, &dist);

        let hasher = Hasher::from("crc32-range-id-5");
        let crc10 = format!("{}{}", path, "crc32_range_id_5_10.log");
        let crc15 = format!("{}{}", path, "crc32_range_id_5_15.log");
        let crc20 = format!("{}{}", path, "crc32_range_id_5_20.log");
        let crc50 = format!("{}{}", path, "crc32_range_id_5_50.log");
        crc32_check(&crc10, &hasher, &dist);
        crc32_check(&crc15, &hasher, &dist);
        crc32_check(&crc20, &hasher, &dist);
        crc32_check(&crc50, &hasher, &dist);

        let hasher = Hasher::from("crc32-range-point");
        let crc10 = format!("{}{}", path, "crc32_range_point_10.log");
        let crc15 = format!("{}{}", path, "crc32_range_point_15.log");
        let crc20 = format!("{}{}", path, "crc32_range_point_20.log");
        let crc50 = format!("{}{}", path, "crc32_range_point_50.log");
        crc32_check(&crc10, &hasher, &dist);
        crc32_check(&crc15, &hasher, &dist);
        crc32_check(&crc20, &hasher, &dist);
        crc32_check(&crc50, &hasher, &dist);

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

    fn raw_check() {
        println!("will check raw hash... ");

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

        println!("check raw hash succeed!");
    }

    fn crc32_check(path: &str, hasher: &Hasher, dist: &Distribute) {
        println!("will check crc32 file: {}", path);

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
                            "crc32 found error - line in java: {}, rust idx: {}",
                            line, idx
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

    struct ConsistentHashInstance {
        consistent_map: BTreeMap<i64, usize>,
        shards: Vec<&'static str>,
    }

    impl ConsistentHashInstance {
        fn from(shards: Vec<&'static str>) -> Self {
            let mut consistent_map = BTreeMap::new();
            for idx in 0..shards.len() {
                let factor = 40;
                for i in 0..factor {
                    let mut md5 = Md5::new();
                    let data: String = shards[idx].to_string() + "-" + &i.to_string();
                    let data_str = data.as_str();
                    md5.input_str(data_str);
                    let mut digest_bytes = [0u8; 16];
                    md5.result(&mut digest_bytes);
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
        let mut md5 = Md5::new();
        md5.input_str(key);
        // let digest_str = md5.result_str();
        let mut out = [0u8; 16];
        md5.result(&mut out);
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
}
