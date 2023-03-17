use ds::time::{Duration, Instant};
use std::{
    fmt::{Debug, Display},
    ops::Deref,
};

use crate::{Command, HashedCommand};

pub type Context = u64;

pub trait Request:
    Debug + Display + Send + Sync + 'static + Unpin + Sized + Deref<Target = HashedCommand>
{
    fn start_at(&self) -> Instant;
    fn last_start_at(&self) -> ds::time::Instant;
    fn elapsed_current_req(&self) -> Duration;
    fn on_noforward(&mut self);
    fn on_sent(self) -> Option<Self>;
    fn on_complete(self, resp: Command);
    fn on_err(self, err: crate::Error);
    fn context_mut(&mut self) -> &mut Context;
    #[deprecated(since = "0.1.0", note = "use context_mut instead")]
    #[inline(always)]
    fn mut_context(&mut self) -> &mut Context {
        self.context_mut()
    }
    // 请求成功后，是否需要进行回写或者同步。
    fn write_back(&mut self, wb: bool);
    //fn is_write_back(&self) -> bool;
    // 请求失败后，是否需要进行重试
    fn try_next(&mut self, goon: bool);
}
