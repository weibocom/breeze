#[cfg(test)]
mod hash_test {
    #![feature(map_first_last)]
    use std::collections::BTreeMap;

    use std::{
        fs::File,
        io::{BufRead, BufReader},
        u64,
    };

    use crypto::digest::Digest;
    use crypto::md5::Md5;
    use hash::{Bkdr, Crc32, Hash};
    use std::ops::Bound::Included;

    #[test]
    fn hash_check() {
        // 将java生成的随机key及hash，每种size都copy的几条过来，用于日常验证
        //let path = "/Users/fishermen/works/weibo/git/java/api-commons/";
        let path = "./records/";
        let bkdr10 = format!("{}{}", path, "bkdr_10.log");
        let bkdr15 = format!("{}{}", path, "bkdr_15.log");
        let bkdr20 = format!("{}{}", path, "bkdr_20.log");
        let bkdr50 = format!("{}{}", path, "bkdr_50.log");
        bkdr_check(&bkdr10);
        bkdr_check(&bkdr15);
        bkdr_check(&bkdr20);
        bkdr_check(&bkdr50);

        let crc10 = format!("{}{}", path, "crc32_10.log");
        let crc15 = format!("{}{}", path, "crc32_15.log");
        let crc20 = format!("{}{}", path, "crc32_20.log");
        let crc50 = format!("{}{}", path, "crc32_50.log");
        crc32_check(&crc10);
        crc32_check(&crc15);
        crc32_check(&crc20);
        crc32_check(&crc50);

        let consis10 = format!("{}{}", path, "consistent_10.log");
        // let consis15 = format!("{}{}", path, "consistent_15.log");
        // let consis20 = format!("{}{}", path, "consistent_20.log");
        // let consis50 = format!("{}{}", path, "consistent_50.log");
        consistent_check(&consis10);
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

        debug_assert!(readers.len() == 4);
        println!("readers: {:?}", readers);
    }

    fn bkdr_check(path: &str) {
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
                    debug_assert!(props.len() == 2);
                    let key = props[0].trim();
                    let hash_in_java = props[1].trim();
                    // println!(
                    //     "{} line: key: {}, hash: {}, hash-len:{}",
                    //     num,
                    //     key,
                    //     hash_in_java,
                    //     hash_in_java.len(),
                    // );

                    let hash = Bkdr {}.hash(key.as_bytes());
                    let hash_in_java_u64 = hash_in_java.parse::<u64>().unwrap();
                    if hash != hash_in_java_u64 {
                        println!(
                            "bkdr found error - line in java: {}, rust hash: {}",
                            line, hash
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

    fn crc32_check(path: &str) {
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
                    debug_assert!(props.len() == 2);
                    let key = props[0].trim();
                    let hash_in_java = props[1].trim();

                    let hash = Crc32 {}.hash(key.as_bytes());
                    let hash_in_java_u64 = hash_in_java.parse::<u64>().unwrap();
                    if hash != hash_in_java_u64 {
                        println!(
                            "crc32 found error - line in java: {}, rust hash: {}",
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
        println!("check crc32 hash from file: {}", path);
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
        let mut instance = ConsistentHashInstance::from(shards);
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
                    debug_assert!(props.len() == 3);
                    let key = props[0].trim();
                    let hash_in_java = props[1].trim();
                    let server_in_java = props[2].trim();
                    let hash_in_java_u64 = hash_in_java.parse::<i64>().unwrap();

                    let (hash, server) = instance.get_hash_server(key);

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
            let mut bk = Bkdr {};
            let h = bk.hash(key.as_bytes()) as usize;
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

            let mut hash = hash.wrapping_rem(i32::MAX as i64);
            if hash < 0 {
                hash = hash.wrapping_mul(-1);
            }
            // let hash_little = LittleEndian::read_i32(&out[j * 4..]) as i64;
            // let hash_big = BigEndian::read_i32(&out[j * 4..]) as i64;
            // println!("raw: {}  {}  {}", hash, hash_little, hash_big);
            // let hash = hash.wrapping_abs() as u64;

            //println!("+++++++ {} - {}", hash, j);
        }
    }
}
