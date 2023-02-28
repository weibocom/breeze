#[cfg(test)]
mod uuid_test {
    use endpoint::mysql::uuid::UuidHelper;
    #[test]
    fn text_id_to_unix_time() {
        let id = 3379782484330149i64;
        let unix_time = UuidHelper::get_unix_time_from_id(id);
        println!("id:{}, unix_time:{}", id, unix_time);
    }
    #[test]
    fn text_id_to_idc() {
        let id = 3379782484330149i64;
        let idc = UuidHelper::get_idc_id_from_id(id);
        println!("id:{}, idc:{}", id, idc);
    }
    #[test]
    fn text_id_to_time() {
        let id = 3379782484330149i64;
        let time = UuidHelper::get_time_from_id(id);
        println!("id:{}, idc:{}", id, time);
    }

    #[test]
    fn text_id_to_biz() {
        let id = 3379782484330149i64;
        let biz = UuidHelper::get_biz_flag(id);
        println!("id:{}, biz:{}", id, biz);
    }
}
