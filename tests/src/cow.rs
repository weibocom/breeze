#[ignore]
#[test]
fn test_cow() {
    use ds::cow;
    let (mut tx, rx) = cow(0usize);
    let mut joins = vec![];
    joins.push(std::thread::spawn(move || {
        let mut n = 0;
        for _ in 0..10000 {
            for _ in 0..10000 {
                n += 1;
                tx.update(n);
            }
            std::thread::sleep(std::time::Duration::from_micros(10));
            // println!("write n={}", n)
        }
    }));
    //从rx中读出来的应该单增
    //本机实验10000,10000 最容易出问题，可能是既有频繁更新，又有适当停顿
    for _ in 0..10 {
        let rx = rx.clone();
        joins.push(std::thread::spawn(move || {
            let mut old = 0;
            for _ in 0..10000 {
                for _ in 0..10000 {
                    let rxn = rx.get();
                    let rxnn = *rxn;
                    assert!(*rxn >= old, "{rxnn} should >= {old}");
                    // println!(" n={n} *rxn={rxnn} old={old}");
                    old = *rxn;
                }
                std::thread::sleep(std::time::Duration::from_micros(10));
                // println!("read n={}", old)
            }
        }));
    }
    for j in joins {
        j.join().unwrap();
    }
}
