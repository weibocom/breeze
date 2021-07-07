use crate::{AsyncReadAll, AsyncWriteAll};

use super::{Receiver, Sender};

use protocol::Protocol;

use futures::ready;

use tokio::io::{AsyncRead, AsyncWrite};

use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

pub async fn copy_bidirectional<A, C, P>(
    agent: A,
    client: C,
    parser: P,
    session_id: usize,
) -> Result<(u64, u64)>
where
    A: AsyncReadAll + AsyncWriteAll + Unpin,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    log::debug!("a new connection received.");
    CopyBidirectional {
        agent: agent,
        client: client,
        parser: parser,
        receiver: Receiver::new(),
        sender: Sender::new(),
        sent: false,
        session_id: session_id,
        seq: 0,
    }
    .await
}

/// 提供一个ping-pong的双向流量copy的Future
/// 1. 从clien流式读取stream,直到能够用parser解析出一个request;
/// 2. 将request发往agent;
/// 3. 从agent读取response
/// 4. 将response发往client;
/// 5. 重复1
/// 任何异常都会导致copy提前路上
struct CopyBidirectional<A, C, P> {
    agent: A,
    client: C,
    parser: P,
    receiver: Receiver,
    sender: Sender,
    sent: bool, // 标识请求是否发送完成。用于ping-pong之间的协调
    seq: usize,
    session_id: usize,
}
impl<A, C, P> Future for CopyBidirectional<A, C, P>
where
    A: AsyncReadAll + AsyncWriteAll + Unpin,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    type Output = Result<(u64, u64)>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let CopyBidirectional {
            agent,
            client,
            parser,
            receiver,
            sender,
            sent,
            seq,
            session_id,
        } = &mut *self;
        let mut client = Pin::new(&mut *client);
        let mut agent = Pin::new(&mut *agent);
        loop {
            if !*sent {
                let bytes = ready!(receiver.poll_copy_one(
                    cx,
                    client.as_mut(),
                    agent.as_mut(),
                    parser,
                    *session_id,
                    *seq
                ))?;
                if bytes == 0 {
                    log::debug!("io-bidirectional not request polled");
                    break;
                }
                *sent = true;
                log::debug!(
                    "io-bidirectional. poll request from clien. bytes:{} seq:{}-{}",
                    bytes,
                    *seq,
                    *session_id
                );
            }
            let bytes = ready!(sender.poll_copy_one(cx, agent.as_mut(), client.as_mut(), parser))?;
            log::debug!(
                "io-bidirectional. write response to client bytes :{} seq:{}-{}",
                bytes,
                *seq,
                *session_id
            );
            *sent = false;
            *seq += 1;
        }

        Poll::Ready(Ok((0, 0)))
    }
}
