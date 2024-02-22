use chrono::Local;

#[test]
fn test_assert() {
    ds::assert!(1 == 1, "assert true");
}

#[test]
fn time_test() {
    let now = Local::now().format("%Y-%m-%d %H:%M:%S.%3f");
    println!("now: {}", now);
}
