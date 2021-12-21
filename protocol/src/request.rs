use crate::callback::CallbackContext;
use crate::{Command, Context, Error, HashedCommand, Operation};
use std::fmt::{self, Debug, Display, Formatter};
pub struct Request {
    ctx: *mut CallbackContext,
}

impl crate::Request for Request {
    #[inline(always)]
    fn len(&self) -> usize {
        self.req().len()
    }
    #[inline(always)]
    fn read(&self, oft: usize) -> &[u8] {
        self.req().read(oft)
    }
    #[inline(always)]
    fn operation(&self) -> Operation {
        self.req().operation()
    }
    #[inline(always)]
    fn hash(&self) -> u64 {
        self.req().hash()
    }
    #[inline(always)]
    fn sentonly(&self) -> bool {
        self.req().sentonly()
    }
    #[inline(always)]
    fn on_sent(&mut self) {
        self.ctx().on_sent();
    }
    #[inline(always)]
    fn on_complete(self, resp: Command) {
        self.ctx().on_complete(resp);
    }
    #[inline(always)]
    fn on_err(self, err: Error) {
        self.ctx().on_err(err);
    }
    #[inline(always)]
    fn mut_context(&mut self) -> &mut Context {
        self.ctx().ctx.as_mut_flag()
    }
    #[inline(always)]
    fn write_back(&mut self, wb: bool) {
        self.ctx().ctx.write_back(wb);
    }
    #[inline(always)]
    fn try_next(&mut self, goon: bool) {
        self.ctx().ctx.try_next(goon);
    }
}
impl Request {
    #[inline(always)]
    pub fn new(ctx: *mut CallbackContext) -> Self {
        Self { ctx }
    }
    #[inline(always)]
    pub fn start(self) {
        self.ctx().start()
    }

    #[inline(always)]
    fn req(&self) -> &HashedCommand {
        self.ctx().request()
    }
    #[inline(always)]
    fn ctx(&self) -> &mut CallbackContext {
        unsafe { &mut *self.ctx }
    }
}

impl Clone for Request {
    fn clone(&self) -> Self {
        panic!("request sould never be cloned!");
    }
}
impl Display for Request {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.ctx())
    }
}
impl Debug for Request {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

unsafe impl Send for Request {}
unsafe impl Sync for Request {}
