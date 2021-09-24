#[cfg(test)]
mod bit_map_test {
    #[derive(Clone)]
    struct T {
        name: String,
        cfg: String,
    }
    impl ds::Update<(&str, &str)> for T {
        fn update(&mut self, o: &(&str, &str)) {
            self.name = o.0.to_string();
            self.cfg = o.1.to_string();
        }
    }
    #[test]
    fn test_cow() {
        let t = T {
            name: "icy0".to_string(),
            cfg: "cfg0".to_string(),
        };
        let (mut tx, rx) = ds::cow(t);
        rx.read(|o| {
            assert_eq!(o.name, "icy0");
            assert_eq!(o.cfg, "cfg0");
        });

        tx.write(&("icy1", "cfg1"));

        rx.read(|o| {
            assert_eq!(o.name, "icy1");
            assert_eq!(o.cfg, "cfg1");
        });
    }
}
