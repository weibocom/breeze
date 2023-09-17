struct Instant(ds::time::Instant);
impl Instant {
    fn now() -> Instant {
        Instant(ds::time::Instant::now())
    }
    fn cycles(&self) -> u64 {
        unsafe { *(&self.0 as *const _ as *const u64) }
    }
}

#[test]
fn check_tsc() {
    let interval = std::time::Duration::from_millis(10);
    let mut last = check_cps(interval);
    let mut n = 0;
    loop {
        let cps = check_cps(interval);
        if (cps - last).abs() / cps < 0.000001 {
            println!("{} cps in {n} turn", cps);
            break;
        }
        n += 1;
        last = cps;
    }
}

// cpu cycles per second
fn check_cps(duration: std::time::Duration) -> f64 {
    let start = Instant::now();
    loop {
        let end = Instant::now();
        if end.0 - start.0 > duration {
            return (end.cycles() - start.cycles()) as f64 / duration.as_secs_f64();
        }
    }
}
