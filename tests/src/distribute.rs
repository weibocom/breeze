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
}
