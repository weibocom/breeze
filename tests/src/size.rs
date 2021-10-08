#[cfg(test)]
mod size_test {
    #[test]
    fn test_size() {
        use crossbeam_channel::{bounded, Receiver, Sender};
        use std::mem::size_of;
        use stream::{Request, RingBufferStream};
        println!("size of RingBufferStream:{}", size_of::<RingBufferStream>());
        println!("size of request:{}", size_of::<Request>());
    }
}
