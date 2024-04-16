#[test]
#[cfg(target_os = "linux")]
fn cpu_type() {
    use context;
    let cpu_type = context::cpu_type();
    assert!(cpu_type.is_ok());
    println!("cpu type: {}", cpu_type.unwrap());
}
