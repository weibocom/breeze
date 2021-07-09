mod init;

pub use init::init;

#[allow(unused_macros)]
#[cfg(not(debug_assertions))]
#[macro_export]
macro_rules! debug {
    ($( $args:expr ),*) => {
        ()
    };
}

#[cfg(debug_assertions)]
#[macro_export]
macro_rules! debug {
    ($( $args:expr ),*) => {
        //println!( $( $args ),* );
        ()
    };
}

#[macro_export]
macro_rules! info {
    ($( $args:expr ),*) => {
           println!( $( $args ),* );
    }
}
#[macro_export]
macro_rules! warn{
    ($( $args:expr ),*) => { println!( $( $args ),* ); }
}
#[macro_export]
macro_rules! error{
    ($( $args:expr ),*) => { println!( $( $args ),* ); }
}

#[macro_export]
macro_rules! einfo {
    ($( $args:expr ),*) => {
           elog::info!( $( $args ),* );
    }
}
