use crate::{AsyncReadAll, AsyncWriteAll};

use super::{Receiver, Sender};

use protocol::{Protocol, RequestId};

use futures::ready;

use tokio::io::{AsyncRead, AsyncWrite};

use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use metrics::MetricsSender;

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
        rid: RequestId::from(session_id, 0),
        request_start: Instant::now(),
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
    rid: RequestId,
    request_start: Instant,
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
            rid,
            request_start,
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
                    rid,
                ))?;
                if bytes == 0 {
                    log::debug!("io-bidirectional not request polled {:?}", rid);
                    break;
                }
                *sent = true;
                *request_start = Instant::now();
                log::debug!(
                    "io-bidirectional request sent to agent. len:{} {:?}",
                    bytes,
                    rid
                );
            }
            let _bytes =
                ready!(sender.poll_copy_one(cx, agent.as_mut(), client.as_mut(), parser, rid))?;
            log::debug!(
                "io-bidirectional. one response sent to client len:{} {:?}",
                _bytes,
                *rid
            );
            *sent = false;
            rid.incr();
            let cost = request_start.elapsed().as_micros();
            MetricsSender::avg("cost".parse().unwrap(), cost as usize);
            MetricsSender::sum("count".parse().unwrap(), 1 as usize);
        }

        Poll::Ready(Ok((0, 0)))
    }
}
