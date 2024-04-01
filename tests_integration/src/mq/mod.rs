use crate::mc_helper::mc_get_text_conn;

/// 测试场景：写入mq消息，然后再读出，存在读放大，但一定得能读出来

const MQ: &str = "mq";

#[test]
fn mq_write_read() {
    let mq_client = mc_get_text_conn(MQ);

    let key = "key1";
    let count = 1000;
    const QSIZES: [usize; 3] = [512, 4096, 32768];

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

/// 构建所需长度的msg
fn build_msg(len: usize) -> String {
    let mut msg = format!("msg-{} ", len);
    for _ in 0..(len - msg.len()) {
        msg.push('a');
    }
    msg
}
