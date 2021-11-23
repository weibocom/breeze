#[cfg(test)]
mod tests {
    use ds::Slice;
    use protocol::Protocol;

    #[test]
    fn test_parse_request() {
        println!("begin");
        let get_request = Slice::from("get key1\r\n".as_ref());
        let get_not_supported_request = Slice::from("get key1 key2 key3 key4\r\n".as_ref());
        let gets_request = Slice::from("gets key1 key2 key3 key4\r\n".as_ref());
        let set_request = Slice::from("set key1 0 3600 6\r\nvalue1\r\n".as_ref());
        let version_request = Slice::from("version\r\n".as_ref());

        let parser = protocol::memcache::MemcacheText::new();
        let parser = protocol::redis::RedisText::new();
        let get_parse_result = parser.parse_request(get_request);
        let get_not_supported_parse_result = parser.parse_request(get_not_supported_request);
        let gets_parse_result = parser.parse_request(gets_request);
        let set_parse_result = parser.parse_request(set_request);
        let version_parse_result = parser.parse_request(version_request);

        let get = get_parse_result.unwrap().unwrap();
        println!(
            "get op = {}, keys.len = {}, keys[0] = {}",
            get.operation().name(),
            get.keys().len(),
            String::from_utf8(get.keys()[0].to_vec()).unwrap()
        );

        println!(
            "get not supported is_none: {}",
            get_not_supported_parse_result.unwrap().is_none()
        );

        let gets = gets_parse_result.unwrap().unwrap();
        println!(
            "gets op = {}, keys.len = {}, keys[0] = {}, keys[1] = {}, keys[2] = {}, keys[3] = {}",
            gets.operation().name(),
            gets.keys().len(),
            String::from_utf8(gets.keys()[0].to_vec()).unwrap(),
            String::from_utf8(gets.keys()[1].to_vec()).unwrap(),
            String::from_utf8(gets.keys()[2].to_vec()).unwrap(),
            String::from_utf8(gets.keys()[3].to_vec()).unwrap()
        );

        let set = set_parse_result.unwrap().unwrap();
        println!(
            "set op = {}, keys[0] = {}",
            set.operation().name(),
            String::from_utf8(set.keys()[0].to_vec()).unwrap()
        );

        let version = version_parse_result.unwrap().unwrap();
        println!("version op = {}", version.operation().name());
    }
}
