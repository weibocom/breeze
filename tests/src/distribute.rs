#[cfg(test)]
mod distribute_test {
    use sharding::{
        distribution::Distribute,
        hash::{Hash, Hasher},
    };

    #[test]
    fn range() {
        let shards_count = 8;
        let mut shards = Vec::with_capacity(shards_count);
        let hasher = Hasher::from("crc32-range-id");
        for i in 0..shards_count {
            shards.push(format!("192.168.10.{}", i));
        }
        let dist = Distribute::from("range-512", &shards);

        let key = "0.schv";
        let hash = hasher.hash(&key.as_bytes());
        let idx = dist.index(hash);
        println!("key:{}, hash:{}, idx: {}", key, hash, idx);
    }

    #[test]
    fn mod_range() {
        let shards_count = 16;
        let mut shards = Vec::with_capacity(shards_count);
        let hasher = Hasher::from("crc32-range-id");
        for i in 0..shards_count {
            shards.push(format!("192.168.10.{}", i));
        }
        let dist = Distribute::from("modrange-1024", &shards);

        let key = "1234567890.fri";
        let hash = hasher.hash(&key.as_bytes());
        let idx = dist.index(hash);
        println!("key:{}, hash:{}, idx: {}", key, hash, idx);
    }

    #[test]
    fn splitmod() {
        let shards_count = 16;
        let mut shards = Vec::with_capacity(shards_count);
        let hasher = Hasher::from("crc32-range-id");
        for i in 0..shards_count {
            shards.push(format!("192.168.10.{}", i));
        }
        let dist = Distribute::from("splitmod-32", &shards);

        let key = "1234567890.fri";
        let hash = hasher.hash(&key.as_bytes());
        let idx = dist.index(hash);
        println!("key:{}, hash:{}, idx: {}", key, hash, idx);
    }

    #[test]
    fn slotmod() {
        let shards_count = 4;
        let mut shards = Vec::with_capacity(shards_count);
        let hasher = Hasher::from("crc32");
        for i in 0..shards_count {
            shards.push(format!("192.168.10.{}", i));
        }
        let dist = Distribute::from("slotmod-1024", &shards);

        let key = "1234567890.fri";
        let hash = hasher.hash(&key.as_bytes());
        let idx = dist.index(hash);
        println!("key:{}, hash:{}, idx: {}", key, hash, idx);
    }

    #[test]
    fn secmod() {
        let hasher = Hasher::from("crc32abs");

        // h14243dc752b5beac 对应i64算法为2461123049，i32算法为-1833844247，abs后为1833844247，
        let key = "h14243dc752b5beac".to_string();
        let hash = hasher.hash(&key.as_bytes());
        println!("hash: {}, key:{} ", hash, key);
        assert_eq!(hash, 1833844247);

        let shards = vec![
            "shard_0".to_string(),
            "shard_1".to_string(),
            "shard_2".to_string(),
        ];
        let dist = Distribute::from("secmod", &shards);
        let idx = dist.index(hash);
        println!("idx:{}", idx);
        assert_eq!(idx, 2);
    }
}
