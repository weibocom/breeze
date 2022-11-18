#[macro_export]
macro_rules! assert {
    ($cond:expr $(,)?) => {{ std::assert!($cond $(,)?) }};
    ($cond:expr, $($arg:tt)+) => {{ std::assert!($cond, $($arg)+) }};
}
#[macro_export]
macro_rules! assert_eq {
    ($cond:expr $(,)?) => {{ std::assert!($cond $(,)?) }};
    ($cond:expr, $($arg:tt)+) => {{ std::assert!($cond, $($arg)+) }};
}
