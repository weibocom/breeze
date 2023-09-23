#[cfg(test)]

mod mysql_strategy {
    // use chrono::{DateTime, TimeZone, Utc};
    // use ds::RingSlice;
    // use endpoint::kv::strategy::{Strategist, Strategy};
    use endpoint::kv::uuid::Uuid;
    // use protocol::memcache::Binary;
    // use std::collections::HashMap;

    // const SQL_INSERT: &'static str = "insert into $db$.$tb$ (id, content) values($k$, $v$)";
    // const SQL_UPDATE: &'static str = "update $db$.$tb$ set content=$v$ where id=$k$";
    // const SQL_DELETE: &'static str = "delete from $db$.$tb$ where id=$k$";
    // const SQL_SELECT: &'static str = "select content from $db$.$tb$ where id=$k$";
    // 接口改动要求完整的二进制mc请求
    // #[test]
    // fn test_get_sql() {
    //     let id = 3094373189550081i64;
    //     // let now = chrono::Utc::now().timestamp_millis();
    //     // let id = UuidSimulator::new().generate_id(now); // Tue Sep 18
    //     let id_str = id.to_string();
    //     let id_slice = RingSlice::from(
    //         id_str.as_ptr() as *mut u8,
    //         id_str.len().next_power_of_two(),
    //         0,
    //         id_str.len(),
    //     );

    //     let mut sqls = HashMap::with_capacity(4);
    //     sqls.insert("SQL_SELECT".to_string(), SQL_SELECT.to_string());

    //     let s = Strategist::new("status".to_string(), 32, 8, vec!["__default__".to_string()]);
    //     let sql_cmd = s.build_kvsql(&id_slice, &id_slice);
    //     if sql_cmd != None {
    //         println!("id: {}, sql: {}", id, sql_cmd.unwrap());
    //     }
    // }
    #[test]
    fn text_id_to_unix_secs() {
        let id = 3379782484330149i64;
        let unix_secs = id.unix_secs();
        println!("id: {} , unix_secs: {}", id, unix_secs);
    }
    // #[test]
    // fn text_id_to_idc() {
    //     let id = 3379782484330149i64;
    //     let idc = UuidHelper::get_idc(id);
    //     println!("id: {} , idc: {}", id, idc);
    // }
    // #[test]
    // fn text_id_to_time() {
    //     let id = 3379782484330149i64;
    //     let time = UuidHelper::get_time(id);
    //     println!("id: {} , time: {}", id, time);
    // }

    // #[test]
    // fn text_id_to_biz() {
    //     let id = 3379782484330149i64;
    //     let biz = UuidHelper::get_biz(id);
    //     println!("id: {} , biz: {}", id, biz);
    // }

    #[test]
    fn test_year() {
        use endpoint::kv::kvtime::KVTime;
        use protocol::kv::Strategy;
        let kv_time = KVTime::new(
            "db_name".to_string(),
            16,
            8,
            vec![
                2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018,
                2019, 65535,
            ],
        );

        use ds::RingSlice;
        let id_str = "3379782484330149"; // Tue Nov 15 00:00:00 CST 2011
        let id_slice = RingSlice::from(
            id_str.as_ptr(),
            id_str.len().next_power_of_two(),
            0,
            id_str.len(),
        );
        let key = kv_time.year(&id_slice);
        assert_eq!(key, 2011);

        let id_str = "3396814711554048"; // Tue Jan  1 00:00:00 CST 2012
        let id_slice = RingSlice::from(
            id_str.as_ptr(),
            id_str.len().next_power_of_two(),
            0,
            id_str.len(),
        );
        let key = kv_time.year(&id_slice);
        assert_eq!(key, 2012);

        let id_str = "4323440483893248"; // Tue Jan  1 00:00:00 CST 2019
        let id_slice = RingSlice::from(
            id_str.as_ptr(),
            id_str.len().next_power_of_two(),
            0,
            id_str.len(),
        );
        let key = kv_time.year(&id_slice);
        assert_eq!(key, 2019);

        let id_str = "1808468696629248"; // Sat Jan  1 00:00:00 CST 2000
        let id_slice = RingSlice::from(
            id_str.as_ptr(),
            id_str.len().next_power_of_two(),
            0,
            id_str.len(),
        );
        let key = kv_time.year(&id_slice);
        assert_eq!(key, 9999);

        let id_str = "4852889155534848"; // Sun Jan  1 00:00:00 CST 2023
        let id_slice = RingSlice::from(
            id_str.as_ptr(),
            id_str.len().next_power_of_two(),
            0,
            id_str.len(),
        );
        let key = kv_time.year(&id_slice);
        assert_eq!(key, 9999);
    }
}
