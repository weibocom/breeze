use crate::{AsyncReadAll, AsyncWriteAll, Request, Response, MAX_REQUEST_SIZE};

use protocol::Protocol;

use futures::ready;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use std::io::{Error, ErrorKind, Result};
use std::pin::Pin;
use std::task::{Context, Poll};
pub(super) struct Client<C, P> {
    client: C,
    parser: P,
    // 当前buffer写入的位置
    w: usize,
    cap: usize,
    buff: Vec<u8>,
    // 上一个request的读取位置
    r: usize,
}

impl<C, P> Client<C, P>
where
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    pub fn from(client: C, parser: P) -> Self {
        log::debug!("client inited");
        let init_cap = 2048usize;
        Self {
            client,
            parser,
            buff: vec![0; init_cap],
            w: 0,
            cap: init_cap,
            r: 0,
        }
    }
    pub fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<Request>> {
        let me = &mut *self;
        if me.r > 0 {
            // 有可能是因为pipeline，所以上一次request结束后，可能还有未处理的请求数据
            // 把[r..w]的数据copy到最0位置
            use std::ptr::copy;
            let left = me.w - me.r;
            if left > 0 {
                unsafe {
                    copy(
                        me.buff.as_ptr().offset(me.r as isize),
                        me.buff.as_mut_ptr(),
                        left,
                    );
                }
            }
            me.r = 0;
            me.w = left;
        }
        loop {
            if me.w == me.cap {
                // 说明容量不足。需要扩展
                if me.cap >= MAX_REQUEST_SIZE {
                    return Poll::Ready(Err(Error::new(
                        ErrorKind::ConnectionAborted,
                        "max request size limited: 1mb",
                    )));
                }
                me.extends();
            }
            let mut buff = ReadBuf::new(&mut me.buff[me.w..]);
            ready!(Pin::new(&mut me.client).poll_read(cx, &mut buff))?;
            let read = buff.filled().len();
            me.w += read;
            if read == 0 {
                // EOF
                return Poll::Ready(Ok(Request::default()));
            }
            let (parsed, n) = me.parser.parse_request(&me.buff[me.r..me.w])?;
            if parsed {
                let req = Request::from(&me.buff[me.r..n]);
                me.r = n;
                return Poll::Ready(Ok(req));
            }
        }
    }
    pub fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        response: Response,
    ) -> Poll<Result<()>> {
        let me = &mut *self;
        let mut r = response.into_readers(&me.parser);
        Pin::new(&mut r).poll_write_to(cx, &mut me.client)
    }
    fn extends(&mut self) {
        let cap = self.buff.len();
        debug_assert_eq!(self.w, cap);
        let cap = (cap * 2).min(MAX_REQUEST_SIZE);
        let mut new_buff = vec![0u8; cap];
        use std::ptr::copy_nonoverlapping;
        unsafe { copy_nonoverlapping(self.buff.as_ptr(), new_buff.as_mut_ptr(), self.buff.len()) };
        self.buff.clear();
        self.buff = new_buff;
    }
}
