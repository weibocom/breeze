// 从s复制size个字节到d
// 调用方要关注s至少有size个字节可以读取
use std::io::Result;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub async fn copy_exact<S, D>(s: &mut S, d: &mut D, size: usize) -> Result<()>
where
    S: AsyncRead + Unpin,
    D: AsyncWrite + Unpin,
{
    let mut buf = [0; 2048];
    let mut left = size;
    while left > 0 {
        let read = left.min(buf.len());
        s.read_exact(&mut buf[0..read]).await?;
        d.write_all(&mut buf[0..read]).await?;
        left -= read;
    }
    //d.flush().await?;
    Ok(())
}
