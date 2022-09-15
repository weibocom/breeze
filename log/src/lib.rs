cfg_if::cfg_if! {
    if #[cfg(feature = "enable-log")] {
        mod enable;
        mod init;
        pub use enable::*;
        pub use init::init;
    } else {
        mod disable;
        pub use disable::*;
        pub fn init(path: &str, _l: &str) -> std::io::Result<()> {
            std::fs::create_dir_all(path)?;
            let mut log = std::fs::File::create(format!("{}/breeze.log", path))?;
            use std::io::Write;
            log.write(b"===> log disabled <===")?;
            Ok(())
        }
    }
}
