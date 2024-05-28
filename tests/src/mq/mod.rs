use std::collections::HashSet;

use endpoint::msgque::strategy::Fixed;
/// 验证读写策略，离线策略
use endpoint::msgque::strategy::RoundRobbin;
use endpoint::msgque::ReadStrategy;
use endpoint::msgque::WriteStrategy;
use rand::random;

mod protocol;
/// 轮询读取40次，预期把每个节点都读一遍
#[test]
fn mq_read_strategy() {
    const READER_COUNT: usize = 40;
    let rstrategy = RoundRobbin::new(READER_COUNT);

    let mut count = 0;
    let mut readed = HashSet::with_capacity(READER_COUNT);
    let mut last_idx = Some(random());
    loop {
        count += 1;
        let idx = rstrategy.get_read_idx(last_idx);
        readed.insert(idx);
        last_idx = Some(idx);

        if readed.len() == READER_COUNT {
            // println!("read strategy loop all: {}/{}", count, readed.len());
            break;
        }
        // println!("read strategy - idx:{} {}/{}", idx, count, readed.len());
    }

    assert_eq!(count, readed.len());
    println!("mq read strategy succeed!");
}

#[test]
fn mq_write_strategy() {
    const QUEUE_LEN: usize = 20;
    const QUEUE_SIZE_COUNT: usize = 5;
    const QUEUE_SIZES: [usize; QUEUE_SIZE_COUNT] = [512, 1024, 2048, 4096, 8192];

    // 构建初始化数据，20个ip，每4个ip一个size的队列
    let mut queues = Vec::with_capacity(QUEUE_LEN);
    for i in 0..QUEUE_LEN {
        queues.push(i);
    }
    let mut qsize_pos = Vec::with_capacity(QUEUE_SIZE_COUNT);
    for i in 0..QUEUE_SIZE_COUNT {
        qsize_pos.push((QUEUE_SIZES[i], QUEUE_LEN / QUEUE_SIZE_COUNT * i));
    }
    println!("queues:{:?}", queues);
    println!("qsize_pos:{:?}", qsize_pos);

    // 每个size请求一次，且重复请求，必须把所有ip轮流一遍
    let wstrategy = Fixed::new(QUEUE_LEN, &qsize_pos);
    for i in 0..QUEUE_SIZE_COUNT {
        let msg_size = QUEUE_SIZES[i] - 10;
        let qsize_start = qsize_pos[i].1;

        let retry_count = queues.len() - qsize_start;
        for retry in 0..retry_count {
            let last_idx = match retry {
                0 => None,
                _ => Some(queues[qsize_start + retry - 1]),
            };
            let idx = wstrategy.get_write_idx(msg_size, last_idx);
            assert_eq!(idx, queues[qsize_start + retry]);
            // println!("msg_size:{}, retry:{}, idx:{}", msg_size, retry, idx);
        }
    }
    println!("mq write strategy succeed!");
}
