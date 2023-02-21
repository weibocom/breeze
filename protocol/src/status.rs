use std::sync::atomic::AtomicU8;
//complete: AtomicBool, // 当前请求是否完成
//inited: AtomicBool,   // response是否已经初始化
//async_done: AtomicBool,
//async_mode: bool, // 是否是异步请求
//try_next: bool,   // 请求失败是否需要重试
//write_back: bool, // 请求结束后，是否需要回写。
//first: bool,      // 当前请求是否是所有子请求的第一个
//last: bool,       // 当前请求是否是所有子请求的最后一个
pub struct Status {
    status: AtomicU8,
}
