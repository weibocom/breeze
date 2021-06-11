use std::marker::PhantomData;

use async_trait::async_trait;
use bb8::ManageConnection;
use bb8::PooledConnection;
use net::Stream;
use std::io::Error;
use tokio::net::TcpStream;

mod connection;
pub use connection::Connection;

pub struct Manager<C> {
    addr: String,
    _c: PhantomData<C>,
}

impl<C> Manager<C> {
    pub fn from(addr: &str) -> Self {
        Self {
            addr: addr.to_owned(),
            _c: Default::default(),
        }
    }
}

#[async_trait]
pub trait Check {
    async fn is_valid(&self) -> Result<(), Error> {
        Ok(())
    }
    fn has_broken(&self) -> bool {
        false
    }
}

#[async_trait]
impl<C> ManageConnection for Manager<C>
where
    C: Send + Sync + 'static + From<Stream> + Check,
{
    type Connection = C;
    type Error = Error;
    #[inline]
    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let stream = TcpStream::connect(&self.addr).await?;
        Ok(C::from(stream.into()))
    }

    // 被动处理异常连接
    #[inline]
    async fn is_valid(&self, conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        conn.is_valid().await
    }

    #[inline]
    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        conn.has_broken()
    }
}
