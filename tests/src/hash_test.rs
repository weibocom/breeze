#[cfg(test)]
mod hash_test {

    use sharding::{
        distribution::Distribute,
        hash::{Hash, Hasher},
    };

    #[test]
    fn crc32_hash() {
        let key = "7516310920..uasvw";
        // let key = "123测试中文hash.fri";

        let crc32_hasher = Hasher::from("crc32");
        let h = crc32_hasher.hash(&key.as_bytes());
        println!("crc32 hash: {}", h);

        let crc32_short_hasher = Hasher::from("crc32-short");
        let h_short = crc32_short_hasher.hash(&key.as_bytes());
        println!("crc32-short hash:{}", h_short);

        let crc32_point = Hasher::from("crc32-point");
        let h_point = crc32_point.hash(&key.as_bytes());
        println!("crc32-point hash: {}", h_point);

        let a = 0b10;
        let b = 0b110;
        let c = 0o10;
        println!("a: {}, b: {}, c: {}", a, b, c);

        let rand_hasher = Hasher::from("random");
        let h1 = rand_hasher.hash(&key.as_bytes());
        let h2 = rand_hasher.hash(&key.as_bytes());
        let h3 = rand_hasher.hash(&key.as_bytes());
        let h4 = rand_hasher.hash(&key.as_bytes());
        println!(
            "key:{}, random-h1:{}, h2:{}, h3:{}, h4:{}",
            key, h1, h2, h3, h4
        );

        let rawsuffix_hahser = Hasher::from("rawsuffix-underscore");
        let key_suffix = 123456789;
        let key = format!("abc_{}", key_suffix);
        let hash = rawsuffix_hahser.hash(&key.as_bytes());
        debug_assert_eq!(key_suffix, hash);
        println!("key:{} rawsuffix-underscore hash:{}", key, hash);
    }

    #[test]
    fn context() {
        let mut i = 1024;
        i += 1;
        i += 1;
        println!("u8:{:b}", i);

        // 先将之前的idx位置零，再设置新的idx
        let qid = 0;
        let mask = !(((!0u16) as u64) << 8);
        i &= mask;
        i |= (qid << 8) as u64;

        println!("u8:{:b}", i);
    }

    #[test]
    fn bkdrsub() {
        let hasher = Hasher::from("bkdrsub");

        let key1 = "abc#12345678901234567";
        let hash1 = hasher.hash(&key1.as_bytes());
        println!("key:{}, hash:{}", key1, hash1);
        assert_eq!(hash1, 1108486745);

        let key2 = "abc#12345678901234567_123456";
        let hash2 = hasher.hash(&key2.as_bytes());
        println!("key:{}, hash:{}", key2, hash2);
        assert_eq!(hash2, 1108486745);
    }

    #[test]
    fn crc32abs() {
        let hasher = Hasher::from("crc32abs");
        let key = "h14243dc752b5beac".to_string();
        let crc = hasher.hash(&key.as_bytes());
        println!("crc: {}, key:{} ", crc, key);
        // h14243dc752b5beac 对应i64算法为2461123049，i32算法为-1833844247，abs后为1833844247，
        assert_eq!(crc, 1833844247);
    }

    #[test]
    fn crc64() {
        let hasher = Hasher::from("crc64");
        let servers = vec!["0", "1", "2", "3", "4", "5"];
        let servers = servers.iter().map(|s| s.to_string()).collect();

        let dist = Distribute::from("modula", &servers);
        let key = "hot_band_conf_6041884361";
        let crc: i64 = hasher.hash(&key.as_bytes());
        let idx = dist.index(crc);

        println!("key:{}, crc64: {}, dist: {}", key, crc, idx);
        assert_eq!(-7536761181773004100_i64, crc);
    }

    #[test]
    fn bkdrabscrc32() {
        let hasher = Hasher::from("bkdrabscrc32");
        let key1 = "1234567890#123";
        let key2 = "1234567890Abc";

        let hash1 = hasher.hash(&key1.as_bytes());
        let hash2 = hasher.hash(&key2.as_bytes());
        println!("bkdrabscrc32: {} : {}", hash1, hash2);
        assert_eq!(hash1, hash2);
    }
}
