#[cfg(test)]
mod mc_discovery_test {
    use super::Vintage;
    use url::Url;

    #[test]
    fn test_lookup() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        let vintage_base = Url::parse("http://config.vintage:80/");

        let mut vintage = Vintage::from_url(vintage_base.unwrap());
        let conf_task = vintage.lookup("cache.service2.0.unread.pool.lru.test", "");
        let conf = rt.block_on(conf_task);

        log::info!("lookup result: {:?}", conf);
        assert!(conf.unwrap().len() > 0);
    }
}
