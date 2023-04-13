#[cfg(test)]
mod redis_test {
    use std::collections::{HashMap, HashSet};

    use ds::MemGuard;
    use protocol::Flag;
    #[test]
    fn test_hosts_eq() {
        let hosts1 = create_hosts();
        let hosts2 = create_hosts();
        if hosts1.eq(&hosts2) {
            println!("hosts are equal!");
        } else {
            assert!(false);
        }
    }

    fn create_hosts() -> HashMap<String, HashSet<String>> {
        let mut hosts = HashMap::with_capacity(3);
        for i in 1..10 {
            let h = format!("{}-{}", "host", i);
            let mut ips = HashSet::with_capacity(5);
            for j in 1..20 {
                let ip = format!("ip-{}", j);
                ips.insert(ip);
            }
            hosts.insert(h.clone(), ips);
        }
        hosts
    }

    #[test]
    fn hash_find() {
        let key = "测试123.key";
        let idx = key.find(".").unwrap();
        println!(". is at: {}", idx);
        for p in 0..idx {
            println!("{}:{}", p, key.as_bytes()[p] as char);
        }
        println!("\r\nhash find test ok!");
    }

    #[test]
    fn redis_flag() {
        use protocol::{Bit, HashedCommand, RedisFlager};
        let mut cmd = HashedCommand::new(MemGuard::from_vec(vec![1u8]), 1, Flag::new());
        assert!(!cmd.master_only());
        cmd.set_master_only();
        assert!(cmd.master_only());

        assert!(!cmd.mkey_first());
        cmd.set_mkey_first();
        assert!(cmd.mkey_first());
        //mkey_first所在位
        cmd.ext_mut().clear(32);
        assert!(!cmd.mkey_first());
        //对前面设置的flag没有影响
        assert!(cmd.master_only());
    }
}
