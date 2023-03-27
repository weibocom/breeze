mod distribution;
mod distribution_old;
use std::thread;

#[test]
fn test() {
    let shards: Vec<String> = vec![
        "1".to_owned(),
        "2".to_owned(),
        "3".to_owned(),
        "4".to_owned(),
    ];
    let dists = [
        "modula",
        "absmodula",
        "range",
        "modrange-256",
        "splitmod-32",
        "slotmod-1024",
    ];
    for dist in dists {
        let dist_new = distribution::Distribute::from(dist, &shards);
        let dist_old = distribution_old::Distribute::from(dist, &shards);
        thread::spawn(move || {
            println!("start {dist}");
            for i in 0..=u32::MAX {
                assert_eq!(dist_new.index(i as i64), dist_old.index(i as i64));
            }
            println!("finish {dist}");
        });
    }

    let shards: Vec<String> = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
    let dist_new = distribution::Distribute::from("modula", &shards);
    let dist_old = distribution_old::Distribute::from("modula", &shards);
    for i in 0..=u32::MAX {
        assert_eq!(dist_new.index(i as i64), dist_old.index(i as i64));
    }
}
