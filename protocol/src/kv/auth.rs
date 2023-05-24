// use crypto::{digest::Digest, sha1::Sha1};

// fn xor<T, U>(mut left: T, right: U) -> T
// where
//     T: AsMut<[u8]>,
//     U: AsRef<[u8]>,
// {
//     left.as_mut()
//         .iter_mut()
//         .zip(right.as_ref().iter())
//         .map(|(l, r)| *l ^= r)
//         .last();
//     left
// }
// //为了能同时接受&[u8]和[u8; 20]作为参数，不知性能是否会有优化
// fn sha1_1(bytes: impl AsRef<[u8]>) -> [u8; 20] {
//     let mut hasher = Sha1::new();
//     hasher.input(bytes.as_ref());
//     let mut out = [0u8; 20];
//     hasher.result(&mut out);
//     out
// }

// fn sha1_2(bytes1: impl AsRef<[u8]>, bytes2: impl AsRef<[u8]>) -> [u8; 20] {
//     let mut hasher = Sha1::new();
//     hasher.input(bytes1.as_ref());
//     hasher.input(bytes2.as_ref());
//     let mut out = [0u8; 20];
//     hasher.result(&mut out);
//     out
// }

// pub(super) fn native_auth(nonce: &[u8], password: &[u8]) -> [u8; 20] {
//     xor(sha1_1(password), sha1_2(nonce, sha1_1(sha1_1(password))))
// }
