use crate::{
    callback::CallbackContext, Attachment, BackendQuota, Command, Context, Error, HashedCommand,
};
use std::{
    fmt::{self, Debug, Display, Formatter},
    ptr::NonNull,
};

pub struct Request {
    ctx: NonNull<CallbackContext>,
}

impl crate::Request for Request {
    #[inline]
    fn start_at(&self) -> ds::time::Instant {
        self.ctx().start_at()
    }
    #[inline]
    fn on_noforward(&mut self) {
        self.ctx().on_noforward();
    }
    #[inline]
    fn on_sent(self) -> Option<Self> {
        if self.ctx().on_sent() {
            Some(self)
        } else {
            None
        }
    }
    #[inline]
    fn on_complete<P: crate::Proto>(self, parser: &P, resp: Command) {
        self.ctx().on_complete(parser, resp);
    }
    #[inline]
    fn on_err(self, err: Error) {
        self.ctx().on_err(err);
    }
    #[inline]
    fn mut_context(&mut self) -> &mut Context {
        &mut self.ctx().flag
    }
    #[inline]
    fn write_back(&mut self, wb: bool) {
        self.ctx().write_back = wb;
    }
    #[inline]
    fn is_write_back(&self) -> bool {
        self.ctx().write_back
    }
    #[inline]
    fn try_next(&mut self, goon: bool) {
        self.ctx().try_next = goon;
    }
    #[inline]
    fn retry_on_rsp_notok(&mut self, retry: bool) {
        self.ctx().retry_on_rsp_notok = retry;
    }
    #[inline]
    fn quota(&mut self, quota: BackendQuota) {
        self.ctx().quota(quota);
    }
    #[inline]
    fn attachment(&self) -> Option<&Attachment> {
        self.ctx().attachment()
    }
    #[inline]
    fn attachment_mut(&mut self) -> &mut Option<crate::Attachment> {
        self.ctx().attachment_mut()
    }
    #[inline]
    fn set_max_tries(&mut self, max_tries: u8) {
        self.ctx().set_max_tries(max_tries);
    }
    // #[inline]
    // fn set_fitst_try(&mut self) {
    //     self.ctx().reset_tries();
    // }
    fn set_next_round(&mut self, next_round: bool) {
        self.ctx().set_next_round(next_round);
    }

    fn get_next_round(&mut self) -> bool {
        self.ctx().get_next_round()
    }
    fn can_split(&mut self) -> bool {
        self.ctx().request.flag().can_split()
    }
}
impl Request {
    #[inline]
    pub fn new(ctx: NonNull<CallbackContext>) -> Self {
        Self { ctx }
    }
    #[inline]
    fn ctx(&self) -> &mut CallbackContext {
        unsafe { &mut *self.ctx.as_ptr() }
    }
}

impl Clone for Request {
    fn clone(&self) -> Self {
        panic!("request sould never be cloned!");
    }
}
impl Display for Request {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ctx())
    }
}
impl Debug for Request {
    #[inline]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

unsafe impl Send for Request {}
unsafe impl Sync for Request {}

use std::ops::{Deref, DerefMut};
impl Deref for Request {
    type Target = HashedCommand;
    #[inline]
    fn deref(&self) -> &Self::Target {
        self.ctx().request()
    }
}
impl DerefMut for Request {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.ctx().request_mut()
    }
}
