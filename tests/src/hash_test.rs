#[cfg(test)]
mod hash_test {

    use sharding::hash::{Hash, HashKey, Hasher};

    #[test]
    fn crc32_short() {
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

        let i = key.find(".").unwrap();
        let i2 = key.as_bytes().utf8_find('.').unwrap();
        use ds::*;
        let slice = RingSlice::from(key.as_ptr(), key.len().next_power_of_two(), 0, key.len());
        let i3 = slice.utf8_find('.').unwrap();
        println!("{}'s delimiter pos:{}/{}/{}", key, i, i2, i3);
    }
}
