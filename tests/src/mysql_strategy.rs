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
    #[test]
    fn test_ymd() {
        let uuid = 3379782484330149_i64; // Tue Nov 15 00:00:00 CST 2011
        let y = uuid.year();
        assert_eq!(y, 2011);
        let (y, m, d) = uuid.ymd();
        assert_eq!(y, 2011);
        assert_eq!(m, 11);
        assert_eq!(d, 15);

        let uuid = 4852889155534848_i64; // Sun Jan  1 00:00:00 CST 2023
        let y = uuid.year();
        assert_eq!(y, 2023);
        let (y, m, d) = uuid.ymd();
        assert_eq!(y, 2023);
        assert_eq!(m, 1);
        assert_eq!(d, 1);
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
}
