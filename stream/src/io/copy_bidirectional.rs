use std::collections::VecDeque;
use std::future::Future;
use std::io::Result;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::ready;
use tokio::io::{AsyncRead, AsyncWrite};

use super::{ConnectStatus, IoMetric, Monitor, Receiver, Sender};
use crate::{AsyncReadAll, AsyncWriteAll, Request};
use discovery::TopologyTicker;
use protocol::{Protocol, RequestId};

pub async fn copy_bidirectional<A, C, P>(
    agent: A,
    client: &mut C,
    parser: P,
    session_id: usize,
    metric_id: usize,
    ticker: TopologyTicker,
) -> Result<ConnectStatus>
where
    A: AsyncReadAll + AsyncWriteAll + Unpin,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    CopyBidirectional {
        agent: agent,
        client: client,
        parser: parser,
        receiver: Receiver::new(metric_id),
        sender: Sender::new(metric_id),
        sent: false,
        rid: RequestId::from(session_id, 0, metric_id),
        metric: IoMetric::from(metric_id),
        checker: Monitor::from(ticker),
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
struct CopyBidirectional<'c, A, C, P> {
    agent: A,
    client: &'c mut C,
    parser: P,
    receiver: Receiver,
    sender: Sender,
    sent: bool, // 标识请求是否发送完成。用于ping-pong之间的协调
    rid: RequestId,

    checker: Monitor,
    // 以下用来记录相关的metrics指标
    metric: IoMetric,
}
impl<'c, A, C, P> Future for CopyBidirectional<'c, A, C, P>
where
    A: AsyncReadAll + AsyncWriteAll + Unpin,
    C: AsyncRead + AsyncWrite + Unpin,
    P: Protocol + Unpin,
{
    type Output = Result<ConnectStatus>;

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
            checker,
        } = &mut *self;
        let mut client = Pin::new(&mut *client);
        let mut agent = Pin::new(&mut *agent);
        let mut direct_response_queue: VecDeque<Request> = VecDeque::new();
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
                    &mut direct_response_queue,
                ))?;
                if metric.req_bytes == 0 {
                    log::debug!("eof:no bytes received {}", rid);
                    break;
                }
                *sent = true;
                log::debug!("req sent.{} {}", metric.req_bytes, rid);
            }
            ready!(sender.poll_copy_one(
                cx,
                agent.as_mut(),
                client.as_mut(),
                parser,
                rid,
                metric,
                &mut direct_response_queue,
            ))?;
            metric.response_done();
            log::debug!("resp sent {} {}", metric.resp_bytes, *rid);
            *sent = false;
            rid.incr();
            // 开始记录metric
            let duration = metric.duration();
            const SLOW: Duration = Duration::from_millis(1);
            if duration >= SLOW {
                log::info!("slow request: {}", metric);
            }
            metrics::duration(metric.op.name(), duration, metric.metric_id);
            metrics::qps("bytes.tx", metric.resp_bytes, metric.metric_id);
            metrics::qps("bytes.rx", metric.req_bytes, metric.metric_id);
            if metric.op.is_retrival() {
                metrics::qps("key", metric.req_keys_num, metric.metric_id);
                metrics::ratio(
                    "hit",
                    (metric.resp_keys_num, metric.req_keys_num),
                    metric.metric_id,
                );
            }
            metric.reset();

            // 说明当前有变更，需要重新建立连接
            if checker.check() {
                return Poll::Ready(Ok(ConnectStatus::Reuse));
            }
        }

        Poll::Ready(Ok(ConnectStatus::EOF))
    }
}
