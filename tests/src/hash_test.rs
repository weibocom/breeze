#[cfg(test)]
mod hash_test {

    use sharding::{
        distribution::Distribute,
        hash::{Hash, Hasher},
    };

    use sharding::hash::*;

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
    fn crc32_hasher() {
        // 执行100次
        // 每次随机生成8~128位的key
        // 对比crc32的hash结果
        let crc32 = Crc32;
        let crc32_hasher = sharding::hash::crc::Crc32::default();
        let crc32_short = Crc32Short;
        let crc32_short_hasher = sharding::hash::crc::Crc32Short::default();
        let crc32_num = Crc32Num::from("crc32-num-1");
        let crc32_num_hasher = sharding::hash::crc::Crc32Num::from(1);
        let delemiter = b'_';
        let crc32_delemiter = Crc32Delimiter::from("crc32-underscore-1");
        let crc32_delemiter_hasher = sharding::hash::crc::Crc32Delimiter::from(1, delemiter);
        let crc32_smartnum = Crc32SmartNum::default();
        let crc32_smartnum_hasher = sharding::hash::crc::Crc32SmartNum::default();
        let crc32_mixnum = Crc32MixNum::default();
        let crc32_mixnum_hasher = sharding::hash::crc::Crc32MixNum::default();
        let crc32_abs = Crc32Abs::default();
        let crc32_abs_hasher = sharding::hash::crc::Crc32Abs::default();
        let crc32_local = Crc32local::default();
        let crc32_local_hasher = sharding::hash::crc::Crc32local::default();
        let crc32_lblocal = LBCrc32localDelimiter::default();
        let crc32_lblocal_hasher = sharding::hash::crc::LBCrc32localDelimiter::default();
        let crc32_lblocal_smartnum = Crc32localSmartNum::default();
        let crc32_lblocal_smartnum_hasher = sharding::hash::crc::Crc32localSmartNum::default();
        let crc32_rawlocal = Rawcrc32local::default();
        let crc32_rawlocal_hasher = sharding::hash::crc::Rawcrc32local::default();
        let crc32_rawsuffix = RawSuffix::from("rawsuffix-underscore");
        let crc32_rawsuffix_hasher = sharding::hash::crc::RawSuffix::from(delemiter);
        use rand::Rng;
        let mut rng = rand::thread_rng();

        for _i in 0..128 {
            let size: usize = rng.gen_range(128..512);
            let mut bytes = (0..size)
                .map(|_| rng.gen_range(b'a'..=b'z'))
                .collect::<Vec<u8>>();
            // 随机写入几个连续的数字，每个数字的长度也是随机的，但不超过8位。
            let runs = 16;
            for _i in 0..runs {
                let pos = rng.gen_range(0..size);
                let len = rng.gen_range(1..8);
                for j in pos..(pos + len).min(bytes.len()) {
                    bytes[j] = rng.gen_range(b'0'..=b'9');
                }

                //随机写入几个分隔符 '.'
                let del_pos = rng.gen_range(0..size);
                bytes[del_pos] = delemiter;
            }

            let key_len = rng.gen_range(4..128);
            bytes.windows(key_len).for_each(|ref key| {
                let crc32_hash = crc32.hash(key);
                let crc32_hasher_hash = crc32_hasher.hash(key);
                assert_eq!(crc32_hash, crc32_hasher_hash);

                // crc32 short
                let crc32_short = crc32_short.hash(key);
                let crc32_short_hasher = crc32_short_hasher.hash(key);
                assert_eq!(crc32_short, crc32_short_hasher);

                // crc32 num
                let crc32_num = crc32_num.hash(key);
                let crc32_num_hasher = crc32_num_hasher.hash(key);
                assert_eq!(crc32_num, crc32_num_hasher);

                // crc32 delemiter
                let crc32_delemiter = crc32_delemiter.hash(key);
                let crc32_delemiter_hasher = crc32_delemiter_hasher.hash(key);
                assert_eq!(crc32_delemiter, crc32_delemiter_hasher, "key:{key:?}");

                // crc32 smart num
                let crc32_smartnum = crc32_smartnum.hash(key);
                let crc32_smartnum_hasher = crc32_smartnum_hasher.hash(key);
                assert_eq!(crc32_smartnum, crc32_smartnum_hasher, "key:{key:?}");

                // crc32 mix num
                let crc32_mixnum = crc32_mixnum.hash(key);
                let crc32_mixnum_hasher = crc32_mixnum_hasher.hash(key);
                assert_eq!(crc32_mixnum, crc32_mixnum_hasher, "key:{key:?}");

                // crc32 abs
                let crc32_abs = crc32_abs.hash(key);
                let crc32_abs_hasher = crc32_abs_hasher.hash(key);
                assert_eq!(crc32_abs, crc32_abs_hasher, "key:{key:?}");

                // crc32 local
                let crc32_local = crc32_local.hash(key);
                let crc32_local_hasher = crc32_local_hasher.hash(key);
                assert_eq!(crc32_local, crc32_local_hasher, "key:{key:?}");

                // crc32 lblocal
                let crc32_lblocal = crc32_lblocal.hash(key);
                let crc32_lblocal_hasher = crc32_lblocal_hasher.hash(key);
                assert_eq!(crc32_lblocal, crc32_lblocal_hasher, "key:{key:?}");

                // crc32 lblocal smartnum
                let crc32_lblocal_smartnum = crc32_lblocal_smartnum.hash(key);
                let crc32_lblocal_smartnum_hasher = crc32_lblocal_smartnum_hasher.hash(key);
                assert_eq!(crc32_lblocal_smartnum, crc32_lblocal_smartnum_hasher);

                // crc32 rawlocal
                let crc32_rawlocal = crc32_rawlocal.hash(key);
                let crc32_rawlocal_hasher = crc32_rawlocal_hasher.hash(key);
                assert_eq!(crc32_rawlocal, crc32_rawlocal_hasher, "key:{key:?}");

                // crc32 rawsuffix
                let crc32_rawsuffix = crc32_rawsuffix.hash(key);
                let crc32_rawsuffix_hasher = crc32_rawsuffix_hasher.hash(key);
                assert_eq!(crc32_rawsuffix, crc32_rawsuffix_hasher, "key:{key:?}");
            });
        }
    }

    #[test]
    fn crc64() {
        let hasher = Hasher::from("crc64");
        let servers = vec!["0", "1", "2", "3", "4", "5"];

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

    #[test]
    fn crc32_num_hasher() {
        let key = [52u8, 48, 53, 54, 100, 105, 111, 120, 113, 113, 100, 101, 99];
        let key = &&key[..];
        let crc32_num = Crc32Num::from("crc32-num-1");
        let crc32_num_hasher = sharding::hash::crc::Crc32Num::from(1);
        let crc32_num = crc32_num.hash(key);
        let crc32_num_hasher = crc32_num_hasher.hash(key);
        assert_eq!(crc32_num, crc32_num_hasher, "key:{key:?}");
    }
    #[test]
    fn crc32_delimiter_hasher() {
        let key = [
            99u8, 97, 113, 119, 111, 46, 112, 120, 121, 46, 113, 121, 54, 51, 98, 109, 113, 122,
            46, 108, 101, 119, 46, 56, 51, 52, 56, 48, 51, 52, 49, 51, 108, 103, 120, 114, 97, 99,
            54, 48, 53, 54, 52, 48, 48, 103, 108, 52, 56, 49, 56, 57, 48, 100, 120, 104, 115, 118,
            110, 54, 50, 50, 48, 51, 52, 100, 117, 105, 97, 102, 51, 51, 50, 51,
        ];
        let key = &&key[..];
        let crc32_delemiter = Crc32Delimiter::from("crc32-point-1");
        let crc32_delemiter_hasher = sharding::hash::crc::Crc32Delimiter::from(1, b'.');
        let crc32_delemiter = crc32_delemiter.hash(key);
        let crc32_delemiter_hasher = crc32_delemiter_hasher.hash(key);
        assert_eq!(crc32_delemiter, crc32_delemiter_hasher, "key:{key:?}");
    }
    #[test]
    fn crc32_smartnum_hasher() {
        let key = [
            57, 48, 50, 54, 106, 46, 119, 99, 104, 109, 114, 98, 115, 46, 121, 116, 114, 108, 122,
            120, 108, 101, 113, 117, 106, 108, 120, 119, 116, 101, 48, 50, 46, 50, 105, 56, 105,
            105, 115, 52, 114, 110, 117, 99, 112, 98, 108, 113, 98, 99, 118, 112, 99, 102, 103,
            118, 53, 46, 51, 54, 52, 112, 102, 117, 114, 107, 107, 120, 97, 112, 111, 106, 100,
            111, 103, 103, 103, 117, 101, 101, 98,
        ];
        println!("key:{:?}", String::from_utf8_lossy(&key[..]));
        let key = &&key[..];
        let crc32_smartnum = Crc32SmartNum::default();
        let crc32_smartnum_hasher = sharding::hash::crc::Crc32SmartNum::default();
        let crc32_smartnum = crc32_smartnum.hash(key);
        let crc32_smartnum_hasher = crc32_smartnum_hasher.hash(key);
        assert_eq!(crc32_smartnum, crc32_smartnum_hasher, "key:{key:?}");
    }
    #[test]
    fn crc32_mixnum_hasher() {
        let key = [104, 121, 46, 49, 53, 56, 51, 55, 120, 105];
        let key = &&key[..];
        let crc32_mixnum = Crc32MixNum::default();
        let crc32_mixnum_hasher = sharding::hash::crc::Crc32MixNum::default();
        let crc32_mixnum = crc32_mixnum.hash(key);
        let crc32_mixnum_hasher = crc32_mixnum_hasher.hash(key);
        assert_eq!(crc32_mixnum, crc32_mixnum_hasher, "key:{key:?}");
    }
    #[test]
    fn crc32_lblocal_hasher() {
        let key = [
            51, 48, 53, 49, 51, 57, 51, 49, 113, 113, 49, 106, 99, 121, 46,
        ];
        println!("key:{:?}", String::from_utf8_lossy(&key[..]));
        let key = &&key[..];
        let crc32_lblocal = LBCrc32localDelimiter::default();
        let crc32_lblocal_hasher = sharding::hash::crc::LBCrc32localDelimiter::default();
        let crc32_lblocal = crc32_lblocal.hash(key);
        let crc32_lblocal_hasher = crc32_lblocal_hasher.hash(key);
        assert_eq!(crc32_lblocal, crc32_lblocal_hasher, "key:{key:?}");
    }
}
