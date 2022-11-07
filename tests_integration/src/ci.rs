pub(crate) mod env {
    pub(crate) trait Mesh {
        fn get_host(&self) -> String;
    }
    impl Mesh for &str {
        fn get_host(&self) -> String {
            use std::path::PathBuf;
            let path: PathBuf = self.into();
            let file_name = path
                .file_name()
                .expect("not valid file")
                .to_str()
                .expect("not valid file");
            let extention = path
                .extension()
                .unwrap_or_default()
                .to_str()
                .unwrap_or_default();
            let file_len = file_name.len() - (extention.len() + (extention.len() > 0) as usize);
            let file_name = &file_name[..file_len];
            let host = std::env::var(file_name);
            assert!(host.is_ok(), "{} is not set", file_name);
            host.expect("resource is not set").to_string()
        }
    }
    pub(crate) fn exists_key_iter() -> std::ops::Range<u64> {
        let min = std::env::var("min_key")
            .unwrap_or_default()
            .parse()
            .unwrap_or(1u64);
        let max = std::env::var("max_key")
            .unwrap_or_default()
            .parse()
            .unwrap_or(10_000u64);
        min..max
    }
    #[test]
    fn test_mesh() {
        // this will panic if the env var is not set
        //let _host = file!().get_host();
    }
}
