#[cfg(test)]
mod mysql_strategy {
    use endpoint::mysql::strategy::Strategy;
    use endpoint::mysql::uuid::UuidHelper;
    use std::collections::HashMap;
    const SQL_INSERT: &'static str = "insert into $db$.$tb$ (id, content) values($k$, $v$)";
    const SQL_UPDATE: &'static str = "update $db$.$tb$ set content=$v$ where id=$k$";
    const SQL_DELETE: &'static str = "delete from $db$.$tb$ where id=$k$";
    const SQL_SELECT: &'static str = "select content from $db$.$tb$ where id=$k$";
    #[test]
    fn test_get_sql() {
        let id = 3379782484330149i64;
        let mut sqls = HashMap::with_capacity(4);
        sqls.insert("SQL_SELECT".to_string(), SQL_SELECT.to_string());

        let s = Strategy::new(
            "status".to_string(),
            "status".to_string(),
            "yymmdd".to_string(),
            32,
            1,
            false,
            sqls,
            "crc32".to_string(),
            "modula".to_string(),
        );
        let mut sql_cmd = s.build_sql("SQL_SELECT", id, id);
        if sql_cmd != None {
            println!("id: {}, sql: {}", id, sql_cmd.unwrap());
        }
    }

    #[test]
    fn text_id_to_unix_time() {
        let id = 3379782484330149i64;
        let unix_time = UuidHelper::get_unix_time_from_id(id);
        println!("id: {} , unix_time: {}", id, unix_time);
    }
    #[test]
    fn text_id_to_idc() {
        let id = 3379782484330149i64;
        let idc = UuidHelper::get_idc_id_from_id(id);
        println!("id: {} , idc: {}", id, idc);
    }
    #[test]
    fn text_id_to_time() {
        let id = 3379782484330149i64;
        let time = UuidHelper::get_time_from_id(id);
        println!("id: {} , time: {}", id, time);
    }

    #[test]
    fn text_id_to_biz() {
        let id = 3379782484330149i64;
        let biz = UuidHelper::get_biz_flag(id);
        println!("id: {} , biz: {}", id, biz);
    }
}
