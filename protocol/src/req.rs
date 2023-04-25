use ds::time::Instant;
use std::fmt::{Debug, Display};
use std::ops::{Deref, DerefMut};

use crate::{Command, HashedCommand};

pub type Context = u64;

pub trait Request:
    Debug
    + Display
    + Send
    + Sync
    + 'static
    + Unpin
    + Sized
    + Deref<Target = HashedCommand>
    + DerefMut<Target = HashedCommand>
{
    // fn cmd(&self) -> &HashedCommand;
    fn cmd_mut(&mut self) -> &mut HashedCommand;

    fn start_at(&self) -> Instant;
    // fn elapsed_current_req(&self) -> Duration;
    //fn operation(&self) -> Operation;
    //fn len(&self) -> usize;
    //fn hash(&self) -> i64;
    // fn update_hash(&mut self, idx_hash: i64);
    fn on_noforward(&mut self);
    fn on_sent(self) -> Option<Self>;
    //fn sentonly(&self) -> bool;
    //fn data(&self) -> &RingSlice;
    //fn read(&self, oft: usize) -> &[u8];
    fn on_complete(self, resp: Command);
    fn on_err(self, err: crate::Error);
    #[inline]
    fn context_mut(&mut self) -> &mut Context {
        self.mut_context()
    }
    fn mut_context(&mut self) -> &mut Context;
    // 请求成功后，是否需要进行回写或者同步。
    fn write_back(&mut self, wb: bool);
    //fn is_write_back(&self) -> bool;
    // 请求失败后，是否需要进行重试
    fn try_next(&mut self, goon: bool);
    // 初始化quota
    fn quota(&mut self, quota: sharding::BackendQuota);
}
