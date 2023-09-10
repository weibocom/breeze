#[test]
fn num_to_str() {
    use ds::NumStr;
    for i in 0..=9999 {
        let num = i.with_str(|s| String::from_utf8_lossy(s).parse::<usize>());
        assert_eq!(Ok(i), num);
    }
    for c in 5..12 {
        let i = 10usize.pow(c);
        let num = i.with_str(|s| String::from_utf8_lossy(s).parse::<usize>());
        assert_eq!(Ok(i), num);
        // 随机验证1000个数
        let mut rng = rand::thread_rng();
        use rand::Rng;
        let start = 10usize.pow(c);
        let end = 10usize.pow(c + 1);
        for _ in 0..1000 {
            let i = rng.gen_range(start..end);
            let num = i.with_str(|s| String::from_utf8_lossy(s).parse::<usize>());
            assert_eq!(Ok(i), num);
        }
    }
}
