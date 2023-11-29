use crate::{
    Command, Commander, Error, Flag, HashedCommand, Metric, MetricItem, Protocol, RequestProcessor,
    Result, Stream, Writer,
};
use ds::ByteOrder;
use sharding::hash::Hash;

#[derive(Clone, Default)]
pub struct Uuid;

impl Protocol for Uuid {
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        _alg: &H,
        process: &mut P,
    ) -> Result<()> {
        let data = stream.slice();
        let mut start = 0usize;
        while let Some(lfcr) = data.find_lf_cr(start) {
            let cmd = stream.take(lfcr + 2 - start);
            start = lfcr + 2;
            let req = HashedCommand::new(cmd, 0, Flag::from_op(0, crate::Operation::Get));
            process.process(req, true);
        }
        Ok(())
    }

    fn parse_response<S: Stream>(&self, stream: &mut S) -> Result<Option<Command>> {
        let data = stream.slice();
        let mut oft = 0usize;
        //正常响应就是三行
        for _ in 0..3 {
            if let Some(lfcr) = data.find_lf_cr(oft) {
                oft = lfcr + 2
            } else {
                return Ok(None);
            }
        }
        //等价于if !data.start_with(0, b"VALU")，但更快，用于判断响应是否乱序
        if data.u32_le(0) != u32::from_le_bytes(*b"VALU") {
            return Err(crate::Error::UnexpectedData);
        }
        return Ok(Some(Command::from_ok(stream.take(oft))));
    }

    fn write_response<C, W, M, I>(
        &self,
        _ctx: &mut C,
        response: Option<&mut Command>,
        w: &mut W,
    ) -> Result<()>
    where
        W: Writer,
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        if let Some(rsp) = response {
            w.write_slice(rsp, 0)?;
            Ok(())
        } else {
            w.write(b"SERVER_ERROR uuid no available\r\n")?;
            Err(Error::Quit)
        }
    }
}
