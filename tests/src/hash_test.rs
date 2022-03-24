#[cfg(test)]
mod hash_test {

    use sharding::hash::{Hash, Hasher};

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
    }
}
