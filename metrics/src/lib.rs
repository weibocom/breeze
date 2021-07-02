mod metrics;

/*
#[cfg(test)]
mod tests {
    use super::metrics::*;
    use std::{thread, time};
    #[test]
    fn test_send() {
        MetricsSender.send(
            "resportal.profile.test".parse().unwrap(),
            "100".parse().unwrap(),
        );
        thread::sleep(time::Duration::from_millis(1000));
    }
}
*/