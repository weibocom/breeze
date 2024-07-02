use crate::mc_helper::mc_get_text_conn;

/// 测试场景：写入mq消息，然后再读出，存在读放大，但一定得能读出来

const MQ: &str = "mq";

#[test]
fn msgque_both_write_read() {
    let mq_client = mc_get_text_conn(MQ);

    let key = "k2";
    let count = 5;
    const QSIZES: [usize; 2] = [512, 4096];

    for i in 0..count {
        let msg_len = QSIZES[i % QSIZES.len()] * 8 / 10;
        let value = build_msg(msg_len);
        println!("will set mcq msg {} with len:{}", i, value.len());
        mq_client.set(key, value, 0).unwrap();
    }

    println!("mq write {} msgs done", count);
    let mut read_count = 0;
    let mut hits = 0;
    loop {
        let msg: Option<String> = mq_client.get(key).unwrap();
        read_count += 1;

        if msg.is_some() {
            hits += 1;
            println!(
                "mq len/{}, hits:{}/{}",
                msg.unwrap().len(),
                hits,
                read_count
            );
            if hits >= count {
                println!("read all mq msgs count:{}/{}", hits, read_count);
                break;
            }
        }
    }
}

#[test]
fn msgque_write() {
    let mq_client = mc_get_text_conn(MQ);

    let key = "k2";
    let count = 10;
    const QSIZES: [usize; 2] = [512, 4096];

    for i in 0..count {
        let msg_len = QSIZES[i % QSIZES.len()] * 8 / 10;
        let value = build_msg(msg_len);
        println!("set mcq msg {} with len:{}/{}", i, value.len(), msg_len);
        mq_client.set(key, value, 0).unwrap();
    }

    println!("mq write {} msgs done", count);
}

#[test]
fn msgque_read() {
    let mq_client = mc_get_text_conn(MQ);

    const COUNT: i32 = 1000;

    let key = "k2";
    let mut read_count = 0;
    let mut hits = 0;
    loop {
        let msg: Result<Option<String>, memcache::MemcacheError> = mq_client.get(key);
        read_count += 1;

        if msg.is_ok() {
            let msg = msg.unwrap();
            // 读空
            if msg.is_none() {
                println!("mq empty, hits:{}/{}", hits, read_count);
                continue;
            }

            // 读到数据
            let msg = msg.unwrap();
            hits += 1;
            println!("mq len/{}, hits:{}/{}", msg.len(), hits, read_count);
            if hits >= COUNT {
                println!("read all mq msgs count:{}/{}", hits, read_count);
                break;
            }
        }
        if read_count > 3 * COUNT {
            println!("stop read for too many empty rs:{}/{}", hits, read_count);
            break;
        }
    }
}

#[test]
fn msgque_strategy_check() {
    let mq_client = mc_get_text_conn(MQ);

    let key = "k2";
    let count = 10;
    const QSIZES: [usize; 1] = [512];

    for i in 0..count {
        let msg_len = QSIZES[i % QSIZES.len()] * 8 / 10;
        let value = build_msg(msg_len);
        println!("will set mcq msg {} with len:{}", i, value.len());
        mq_client.set(key, value, 0).unwrap();
    }

    println!("mq write {} msgs done", count);
    let mut read_count = 0;
    let mut hits = 0;
    loop {
        let msg: Option<String> = mq_client.get(key).unwrap();
        read_count += 1;

        if msg.is_some() {
            hits += 1;
            println!(
                "mq len/{}, hits:{}/{}",
                msg.unwrap().len(),
                hits,
                read_count
            );
            if hits >= count {
                println!("read all mq msgs count:{}/{}", hits, read_count);
                break;
            }
        }
    }

    let hits_percent = (hits as f64) / (read_count as f64);
    assert!(
        hits_percent >= 0.9,
        "check read strategy:{}/{}",
        hits,
        read_count
    );
}

/// 构建所需长度的msg
fn build_msg(len: usize) -> String {
    let mut msg = format!("msg-{} ", len);
    for _ in 0..(len - msg.len()) {
        msg.push('a');
    }
    msg
}
