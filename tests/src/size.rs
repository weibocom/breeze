#[test]
fn test_size() {
    //use crossbeam_channel::{bounded, Receiver, Sender};
    //use std::mem::size_of;
    //use stream::{MpmcStream, Request};
    //println!("size of MpmcStream:{}", size_of::<MpmcStream>());
    //println!("size of request:{}", size_of::<Request>());
}

#[test]
fn leu16() {
    let data = [0_u8, 255];
    let le_u16 = u16::from_le_bytes(data);
    println!("le_u16: {}", le_u16);

    let be_u16 = u16::from_be_bytes(data);
    println!("be_u16: {}", be_u16);
}
