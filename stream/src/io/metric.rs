use protocol::Operation;
use std::time::{Duration, Instant};

pub(crate) struct IoMetric {
    pub(crate) metric_id: usize,
    pub(crate) op: Operation,        // 请求类型
    pub(crate) req_receive: Instant, // 从client收到第一个字节的时间
    pub(crate) req_recv_num: usize,  // 从client接收到一个完整的包调用的io_read的次数
    pub(crate) req_bytes: usize,     // 请求包的大小
    pub(crate) req_done: Instant,    // 请求接收完成消耗的时间
    pub(crate) resp_ready: Instant,  // response准备好开始发送的时间
    pub(crate) resp_sent_num: usize, // 从client接收到一个完整的包调用的io_read的次数
    pub(crate) resp_bytes: usize,    // response包的大小
    pub(crate) resp_done: Instant,   // response发送完成时间
}

impl IoMetric {
    #[inline(always)]
    pub(crate) fn reset(&mut self) {
        self.req_recv_num = 0;
        self.req_bytes = 0;
        self.resp_sent_num = 0;
        self.resp_bytes = 0;
        // 和时间相关的不需要reset。因为每次都是重新赋值
    }

    // 从client接收到n个字节
    #[inline(always)]
    pub(crate) fn req_received(&mut self, _n: usize) {
        if self.req_recv_num == 0 {
            // 是第一次接收
            self.req_receive = Instant::now();
        }
        self.req_recv_num += 1;
        //self.req_bytes += n;
    }
    #[inline(always)]
    pub(crate) fn req_done(&mut self, op: Operation, n: usize) {
        self.req_done = Instant::now();
        self.op = op;
        self.req_bytes = n;
    }

    #[inline(always)]
    pub(crate) fn response_ready(&mut self) {
        if self.resp_sent_num == 0 {
            self.resp_ready = Instant::now();
        }
        self.resp_sent_num += 1;
    }
    // 成功发送了n个字节
    #[inline(always)]
    pub(crate) fn response_sent(&mut self, n: usize) {
        self.resp_sent_num += 1;
        self.resp_bytes += n;
    }
    #[inline(always)]
    pub(crate) fn response_done(&mut self) {
        self.resp_done = Instant::now();
        // 因为response_ready可能会被调用多次，为了方便，在ready里面对num进行了+1。
        // 所以实际sent的次数会多一
        self.resp_sent_num -= 1;
    }
    // 从接收完第一请求个字节，到最后一个response发送完成的耗时
    pub(crate) fn duration(&self) -> Duration {
        self.resp_done.duration_since(self.req_receive)
    }
    pub(crate) fn from(metric_id: usize) -> Self {
        Self {
            metric_id: metric_id,
            op: Operation::Other,
            req_receive: Instant::now(),
            req_recv_num: 0,
            req_bytes: 0,
            req_done: Instant::now(),
            resp_ready: Instant::now(),
            resp_sent_num: 0,
            resp_bytes: 0,
            resp_done: Instant::now(),
        }
    }
}
