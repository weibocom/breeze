pub trait Response {
    fn ok(&self) -> bool;
}
//use ds::RingSlice;
//
//pub struct Response<C> {
//    seq: usize,
//    cmd: ResponseCmd,
//    // 在释放cmd的时候，回调
//    cb: C,
//}
//impl<C> Response<C> {
//    #[inline(always)]
//    pub fn from(cmd: ResponseCmd, seq: usize, cb: C) -> Self {
//        Self { seq, cmd, cb }
//    }
//}
//pub struct ResponseCmd {
//    flag: u64,
//    cmd: RingSlice,
//}
//
//impl ResponseCmd {
//    //#[inline(always)]
//    //pub fn len(&self) -> usize {
//    //    self.cmd.len()
//    //}
//}
//use std::ops::Deref;
//impl<A> Deref for Response<A> {
//    type Target = ResponseCmd;
//    #[inline]
//    fn deref(&self) -> &Self::Target {
//        &self.cmd
//    }
//}
//
//impl<C> Response<C> {
//    pub fn seq(&mut self, seq: usize) {
//        todo!();
//    }
//    pub fn get_seq(&self) -> usize {
//        todo!();
//    }
//    pub fn len(&self) -> usize {
//        todo!();
//    }
//}
