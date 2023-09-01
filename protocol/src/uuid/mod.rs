use crate::{
    Command, Commander, Flag, HashedCommand, Metric, MetricItem, Protocol, RequestProcessor,
    Result, Stream, Writer,
};
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
        loop {
            if let Some(lfcr) = data.find_lf_cr(start) {
                let cmd = stream.take(lfcr + 2 - start);
                start = lfcr + 2;
                let req = HashedCommand::new(cmd, 0, Flag::new());
                process.process(req, true);
            } else {
                return Ok(());
            }
        }
    }

    fn parse_response<S: Stream>(&self, stream: &mut S) -> Result<Option<Command>> {
        let data = stream.slice();
        if let Some(lfcr1) = data.find_lf_cr(0) {
            if data.start_with(0, b"VALUE") {
                if let Some(lfcr2) = data.find_lf_cr(lfcr1 + 2) {
                    if let Some(lfcr3) = data.find_lf_cr(lfcr2 + 2) {
                        return Ok(Some(Command::from_ok(stream.take(lfcr3 + 2))));
                    }
                }
            } else {
                return Err(crate::Error::UnexpectedData);
            }
        }
        return Ok(None);
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
        } else {
            w.write(b"SERVER_ERROR uuid no available\r\n")?;
        }
        Ok(())
    }
    fn config(&self) -> crate::Config {
        crate::Config {
            pipeline: true,
            ..Default::default()
        }
    }
}
