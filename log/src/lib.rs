cfg_if::cfg_if! {
    if #[cfg(feature = "enable-log")] {
        mod enable;
        mod init;
        pub use enable::*;
        pub use init::init;
    } else {
        mod disable;
        pub use disable::*;
        pub fn init(_path: &str, _l: &str) -> std::io::Result<()> {
            println!("===> log disabled <=== ");
            Ok(())
        }
    }
}
