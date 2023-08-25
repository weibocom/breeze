use crate::{
    Command, Commander, Metric, MetricItem, Protocol, RequestProcessor, Result, Stream, Writer,
};
use sharding::hash::Hash;

#[derive(Clone, Default)]
pub struct Uuid;

impl Protocol for Uuid {
    fn parse_request<S: Stream, H: Hash, P: RequestProcessor>(
        &self,
        stream: &mut S,
        alg: &H,
        process: &mut P,
    ) -> Result<()> {
        todo!()
    }

    fn parse_response<S: Stream>(&self, data: &mut S) -> Result<Option<Command>> {
        todo!()
    }

    fn write_response<C, W, M, I>(
        &self,
        ctx: &mut C,
        response: Option<&mut Command>,
        w: &mut W,
    ) -> Result<()>
    where
        W: Writer,
        C: Commander<M, I>,
        M: Metric<I>,
        I: MetricItem,
    {
        todo!()
    }
    fn config(&self) -> crate::Config {
        todo!()
    }
}
