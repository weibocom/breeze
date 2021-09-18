use crate::{AsyncReadAll, AsyncWriteAll};

use super::{IoMetric, Receiver, Sender};

use protocol::{Operation, Protocol, RequestId};

use futures::ready;

use tokio::io::{AsyncRead, AsyncWrite};

use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

pub async fn copy_bidirectional<A, C, P>(
    agent: A,
    client: C,
    parser: P,
    session_id: usize,
    metric_id: usize,
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
        rid: RequestId::from(session_id, 0, metric_id),
        metric: IoMetric::from(metric_id),
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

    // 以下用来记录相关的metrics指标
    metric: IoMetric,
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
            metric,
        } = &mut *self;
        let mut client = Pin::new(&mut *client);
        let mut agent = Pin::new(&mut *agent);
        loop {
            metric.enter();
            if !*sent {
                ready!(receiver.poll_copy_one(
                    cx,
                    client.as_mut(),
                    agent.as_mut(),
                    parser,
                    rid,
                    metric,
                ))?;
                if metric.req_bytes == 0 {
                    log::debug!("eof:no bytes received {}", rid);
                    break;
                }
                *sent = true;
                log::debug!("req sent.{} {}", metric.req_bytes, rid);
            }
            ready!(sender.poll_copy_one(cx, agent.as_mut(), client.as_mut(), parser, rid, metric))?;
            metric.response_done();
            log::debug!("resp sent {} {}", metric.resp_bytes, *rid);
            *sent = false;
            rid.incr();
            // 开始记录metric
            let duration = metric.duration();
            const SLOW: Duration = Duration::from_millis(32);
            if duration >= SLOW {
                log::info!("slow request: {}", metric);
            }
            metrics::duration(metric.op.name(), duration, metric.metric_id);
            metrics::qps("bytes.tx", metric.resp_bytes, metric.metric_id);
            metrics::qps("bytes.rx", metric.req_bytes, metric.metric_id);
            match metric.op {
                Operation::Get | Operation::Gets => {
                    metrics::qps("key", metric.req_keys_num, metric.metric_id);
                    metrics::ratio(
                        "hit",
                        (metric.resp_keys_num, metric.req_keys_num),
                        metric.metric_id,
                    );
                }
                _ => {}
            }
            metric.reset();
        }

        Poll::Ready(Ok((0, 0)))
    }
}
