use crate::{AsyncReadAll, AsyncWriteAll};

use super::{Receiver, Sender};

use protocol::Protocol;

use futures::ready;

use tokio::io::{AsyncRead, AsyncWrite};

use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};

pub async fn copy_bidirectional<A, C, P>(agent: A, client: C, parser: P) -> Result<(u64, u64)>
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
        } = &mut *self;
        let mut client = Pin::new(&mut *client);
        let mut agent = Pin::new(&mut *agent);
        let recv = receiver.poll_copy(cx, client.as_mut(), agent.as_mut(), parser)?;
        let send = sender.poll_copy(cx, agent, client, parser)?;

        let _rx = ready!(recv);
        let _tx = ready!(send);
        Poll::Ready(Ok((0, 0)))
    }
}
