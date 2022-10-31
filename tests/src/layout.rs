// 不要轻易变更这里面的测试用例，除非你知道你在做什么。拉相关同学进行方案评审。
use std::{mem::size_of, sync::atomic::AtomicU32};
#[test]
fn check_layout() {
    assert_eq!(32, size_of::<ds::RingSlice>());
    assert_eq!(8, size_of::<protocol::Context>());
    assert_eq!(216, size_of::<protocol::callback::CallbackContext>());
    assert_eq!(
        size_of::<protocol::Context>(),
        size_of::<protocol::redis::RequestContext>()
    );
    assert_eq!(24, size_of::<protocol::Flag>());
    assert_eq!(1, size_of::<protocol::Resource>());
    assert_eq!(240, size_of::<stream::buffer::StreamGuard>());
    assert_eq!(56, size_of::<ds::queue::PinnedQueue<AtomicU32>>());
    assert_eq!(16, size_of::<metrics::Metric>());
    assert_eq!(64, size_of::<metrics::Item>());
}
