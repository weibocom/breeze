mod get_sync;
mod multi_get;
// mod multi_get_2;
mod operation;
mod pipeline;
mod request_ref;
mod route;
mod set_sync;
mod sharding;

pub use get_sync::AsyncGetSync;
pub use multi_get::AsyncMultiGetSharding;
// pub use multi_get_2::AsyncMultiGet;
pub use operation::AsyncOperation;
pub use pipeline::PipeToPingPongChanWrite;
pub use request_ref::RequestRef;
pub use route::AsyncRoute;
pub use set_sync::AsyncSetSync;
pub use sharding::AsyncSharding;

use tokio::io::AsyncWrite;

/// 该接口是一个marker接口。实现了该接口的AsyncWrite，本身不
/// 会处理buf数据，只会把数据会给chan的接收方，但在数据会给
/// 下游之前，会确保buf是一个完整的request请求。request的格式
/// 由具体的协议决定。方便下由处理。
/// 通常实现要尽可能确保chan处理buf的过程是zero copy的。
/// 输入是pipeline的，输出是ping-pong的。
pub trait AsyncPipeToPingPongChanWrite: AsyncWrite + Unpin {}

/// 标识一个实现了AsyncWrite的接口，写入buf时，只能有以下情况:
/// buf全部写入成功
/// Pending
/// 写入错误
/// 不能出现部分写入成功的情况。方案处理
pub trait AsyncWriteAll {}

/// 确保读取response的时候，类似于NotFound、Stored这样的数据包含
/// 在一个readbuf中，不被拆开，方便判断

impl AsyncWriteAll for tokio::net::TcpStream {}
impl AsyncWriteAll for tokio::net::tcp::OwnedWriteHalf {}
