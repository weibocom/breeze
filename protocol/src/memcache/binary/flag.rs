// // 0: sentonly
// const SENTONLY_BIT: u8 = 1;
// pub trait McFlager {
//     fn set_sentonly(&mut self, sentonly: bool);
//     fn sentonly(&self) -> bool;
// }

// use crate::BitOP;
// impl McFlager for u64 {
//     #[inline]
//     fn set_sentonly(&mut self, sentonly: bool) {
//         self.bit_set(sentonly, 0)
//     }
//     #[inline]
//     fn sentonly(&self) -> bool {
//         self.bit_get(0)
//     }
// }
